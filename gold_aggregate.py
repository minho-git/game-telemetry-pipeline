"""
Silver -> Gold 집계

목적:
- Silver의 사건 단위(킬 하나 = 한 행) 데이터를, 비즈니스 질문에 바로 답이 되는
  요약 지표로 압축한다.
  - player_stats: 플레이어별 K/D, 선호 무기, 현재 티어
  - match_summary: 매치별 요약(맵/모드/지속시간/총 킬 수)
  - item_category_stats: 아이템 카테고리별 습득 건수

account_id 결측 처리:
- Bronze 단계에서 의도적으로 넣어둔 account_id 결측 이벤트는, 플레이어별
  집계에서는 "누구인지 모르는" 데이터라 의미가 없으므로 제외한다 (Silver에는
  원본 그대로 남아있어서 필요하면 언제든 다시 볼 수 있다).
"""

from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, row_number, when

PROJECT_ROOT = Path(__file__).parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("gold-aggregate").getOrCreate()


def build_player_stats(spark: SparkSession):
    kills = spark.read.parquet(str(SILVER_DIR / "kills"))
    profile_scd = spark.read.parquet(str(SILVER_DIR / "player_profile_scd"))

    kill_counts = (
        kills.filter(col("attacker_account_id").isNotNull())
        .groupBy(col("attacker_account_id").alias("account_id"))
        .agg(count("*").alias("kill_count"))
    )
    death_counts = (
        kills.filter(col("victim_account_id").isNotNull())
        .groupBy(col("victim_account_id").alias("account_id"))
        .agg(count("*").alias("death_count"))
    )

    player_kd = kill_counts.join(death_counts, on="account_id", how="outer").fillna(
        0, subset=["kill_count", "death_count"]
    )
    player_kd = player_kd.withColumn(
        "kd_ratio",
        when(col("death_count") == 0, col("kill_count").cast("double")).otherwise(
            col("kill_count") / col("death_count")
        ),
    )

    # 선호 무기: 플레이어별 무기 사용 킬 수를 세고, 가장 많이 쓴 무기(1등)만 남긴다
    weapon_counts = (
        kills.filter(col("attacker_account_id").isNotNull())
        .groupBy(col("attacker_account_id").alias("account_id"), "weapon")
        .agg(count("*").alias("weapon_kill_count"))
    )
    w = Window.partitionBy("account_id").orderBy(col("weapon_kill_count").desc())
    favorite_weapon = (
        weapon_counts.withColumn("rank", row_number().over(w))
        .filter(col("rank") == 1)
        .select("account_id", col("weapon").alias("favorite_weapon"))
    )

    current_profile = profile_scd.filter(col("is_current")).select(
        "account_id", "nickname", col("tier").alias("current_tier")
    )

    player_stats = (
        player_kd.join(favorite_weapon, on="account_id", how="left")
        .join(current_profile, on="account_id", how="left")
    )
    return player_stats


def build_match_summary(spark: SparkSession):
    matches = spark.read.parquet(str(SILVER_DIR / "matches"))
    kills = spark.read.parquet(str(SILVER_DIR / "kills"))

    kill_counts_per_match = kills.groupBy("match_id").agg(count("*").alias("total_kills"))

    match_summary = matches.join(kill_counts_per_match, on="match_id", how="left").fillna(
        0, subset=["total_kills"]
    )
    return match_summary.select(
        "match_id", "map_name", "mode", "team_size",
        "duration_seconds", "survivor_count", "is_complete", "total_kills",
    )


def build_item_category_stats(spark: SparkSession):
    pickups = spark.read.parquet(str(SILVER_DIR / "pickups"))
    return pickups.groupBy("category").agg(count("*").alias("pickup_count"))


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    player_stats = build_player_stats(spark)
    match_summary = build_match_summary(spark)
    item_category_stats = build_item_category_stats(spark)

    player_stats.write.mode("overwrite").parquet(str(GOLD_DIR / "player_stats"))
    match_summary.write.mode("overwrite").parquet(str(GOLD_DIR / "match_summary"))
    item_category_stats.write.mode("overwrite").parquet(str(GOLD_DIR / "item_category_stats"))

    print(
        f"Gold 집계 완료: player_stats {player_stats.count()}행, "
        f"match_summary {match_summary.count()}행, "
        f"item_category_stats {item_category_stats.count()}행"
    )


if __name__ == "__main__":
    main()
