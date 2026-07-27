"""
Bronze -> Silver 변환

목적:
- Bronze는 이벤트 타입이 다 섞인 채로, payload가 문자열 그대로 쌓여있다.
- Silver에서 이벤트 타입별로 필터링 + payload 파싱을 해서 실제로 조회 가능한
  팩트 테이블(kills/damages/pickups/matches)로 만들고, 정합성/유실/SCD까지
  처리한다.

배치로 처리하는 이유:
- dedup(전체 이력과 비교)이나 SCD 병합(플레이어 전체 시즌을 봐야 함)처럼
  "전체를 봐야 하는" 로직이라, 증분 스트리밍보다 배치 재계산이 단순하고
  안전하다 (Bronze->Silver를 스트리밍으로 하는 것도 가능하지만, 지금 데이터
  규모에서는 배치가 더 간단한 선택).
"""

from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lag,
    lead,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

PROJECT_ROOT = Path(__file__).parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
PLAYER_PROFILE_PATH = PROJECT_ROOT / "data" / "raw" / "player_profile_seasons.json"

MATCH_START_SCHEMA = StructType(
    [
        StructField("map_name", StringType()),
        StructField("mode", StringType()),
        StructField("team_size", IntegerType()),
    ]
)
MATCH_END_SCHEMA = StructType(
    [
        StructField("duration_seconds", IntegerType()),
        StructField("survivor_count", IntegerType()),
    ]
)
KILL_SCHEMA = StructType(
    [
        StructField("attacker_account_id", StringType()),
        StructField("attacker_nickname", StringType()),
        StructField("victim_account_id", StringType()),
        StructField("victim_nickname", StringType()),
        StructField("weapon", StringType()),
        StructField("distance", DoubleType()),
    ]
)
DAMAGE_SCHEMA = StructType(
    [
        StructField("attacker_account_id", StringType()),
        StructField("victim_account_id", StringType()),
        StructField("damage", DoubleType()),
        StructField("damage_type", StringType()),
    ]
)
PICKUP_SCHEMA = StructType(
    [
        StructField("account_id", StringType()),
        StructField("nickname", StringType()),
        StructField("item_id", StringType()),
        StructField("category", StringType()),
    ]
)


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("silver-transform").getOrCreate()


def load_bronze(spark: SparkSession):
    """Bronze 전체를 읽고 event_id 기준 dedup. (총 처리 건수, 제거된 중복 건수도 같이 반환)"""
    bronze = spark.read.parquet(str(BRONZE_DIR))
    total_before = bronze.count()
    deduped = bronze.dropDuplicates(["event_id"])
    total_after = deduped.count()
    return deduped, total_after, total_before - total_after


def parse_event(bronze, event_type: str, schema: StructType):
    """특정 이벤트 타입만 필터링해서 payload를 스키마대로 펼친다."""
    return (
        bronze.filter(col("event_type") == event_type)
        .select(
            "event_id",
            "match_id",
            "event_time",
            from_json(col("payload"), schema).alias("p"),
        )
        .select("event_id", "match_id", "event_time", "p.*")
    )


def build_matches(bronze):
    """LogMatchStart와 LogMatchEnd를 match_id로 합쳐 매치당 한 행으로 만든다.
    left join이라, 끝나지 않은(유실 의심) 매치는 종료 관련 컬럼이 null로 남는다."""
    starts = (
        bronze.filter(col("event_type") == "LogMatchStart")
        .select(
            "match_id",
            col("event_time").alias("match_start_time"),
            from_json(col("payload"), MATCH_START_SCHEMA).alias("p"),
        )
        .select("match_id", "match_start_time", "p.*")
    )
    ends = (
        bronze.filter(col("event_type") == "LogMatchEnd")
        .select(
            "match_id",
            col("event_time").alias("match_end_time"),
            from_json(col("payload"), MATCH_END_SCHEMA).alias("p"),
        )
        .select("match_id", "match_end_time", "p.*")
    )

    matches = starts.join(ends, on="match_id", how="left")
    return matches.withColumn("is_complete", col("match_end_time").isNotNull())


def build_player_profile_scd(spark: SparkSession):
    """시즌별 티어 스냅샷을 SCD Type 2로 병합.
    티어가 안 바뀐 연속 시즌은 한 구간으로 합치고, 바뀐 시점마다 새 구간을 만든다."""
    profiles = spark.read.option("multiLine", True).json(str(PLAYER_PROFILE_PATH))

    w = Window.partitionBy("account_id").orderBy("snapshot_date")
    tagged = profiles.withColumn("prev_tier", lag("tier").over(w))
    tagged = tagged.withColumn(
        "is_change",
        when(col("prev_tier").isNull() | (col("prev_tier") != col("tier")), 1).otherwise(0),
    )
    tagged = tagged.withColumn("change_group", spark_sum("is_change").over(w))

    grouped = tagged.groupBy("account_id", "nickname", "tier", "change_group").agg(
        spark_min("snapshot_date").alias("valid_from")
    )

    w2 = Window.partitionBy("account_id").orderBy("valid_from")
    scd = grouped.withColumn("valid_to", lead("valid_from").over(w2))
    scd = scd.withColumn("is_current", col("valid_to").isNull())
    return scd.drop("change_group")


def build_quality_metrics(spark: SparkSession, total_events, duplicate_count, total_matches, incomplete_matches):
    loss_ratio = incomplete_matches / total_matches if total_matches else 0.0
    return spark.createDataFrame(
        [
            {
                "total_events": total_events,
                "duplicate_count": duplicate_count,
                "total_matches": total_matches,
                "incomplete_matches": incomplete_matches,
                "loss_ratio": loss_ratio,
            }
        ]
    ).withColumn("run_timestamp", current_timestamp())


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze, total_events, duplicate_count = load_bronze(spark)

    matches = build_matches(bronze).cache()
    total_matches = matches.count()
    incomplete_matches = matches.filter(col("is_complete") == False).count()  # noqa: E712

    kills = parse_event(bronze, "LogPlayerKillV2", KILL_SCHEMA)
    damages = parse_event(bronze, "LogPlayerTakeDamage", DAMAGE_SCHEMA)
    pickups = parse_event(bronze, "LogItemPickup", PICKUP_SCHEMA)
    player_profile_scd = build_player_profile_scd(spark)
    quality_metrics = build_quality_metrics(
        spark, total_events, duplicate_count, total_matches, incomplete_matches
    )

    matches.write.mode("overwrite").parquet(str(SILVER_DIR / "matches"))
    kills.write.mode("overwrite").parquet(str(SILVER_DIR / "kills"))
    damages.write.mode("overwrite").parquet(str(SILVER_DIR / "damages"))
    pickups.write.mode("overwrite").parquet(str(SILVER_DIR / "pickups"))
    player_profile_scd.write.mode("overwrite").parquet(str(SILVER_DIR / "player_profile_scd"))
    quality_metrics.write.mode("append").parquet(str(SILVER_DIR / "quality_metrics"))

    print(
        f"Silver 변환 완료: 이벤트 {total_events}건(중복 {duplicate_count}건 제거), "
        f"매치 {total_matches}건 중 유실 의심 {incomplete_matches}건 "
        f"(유실률 {incomplete_matches / total_matches:.1%})"
    )


if __name__ == "__main__":
    main()
