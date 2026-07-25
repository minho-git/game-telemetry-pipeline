"""
Kafka Consumer -> Bronze 레이어 적재 (Spark Structured Streaming)

목적:
- Kafka topic("pubg-match-events")을 Structured Streaming으로 읽어서 Bronze
  레이어에 Parquet로 저장한다. payload는 이벤트 타입별로 필드가 달라서 여기서는
  파싱하지 않고 문자열 그대로 저장한다 (파싱은 Silver의 역할).

trigger(availableNow=True):
- 무한정 떠있는 스트리밍 잡이 아니라, 지금까지 쌓인 데이터를 전부 처리하고
  종료하는 배치성 실행 방식. Airflow가 이 스크립트를 주기적으로 실행시키는
  구조에 맞다.

체크포인트(checkpointLocation):
- 처리한 Kafka 오프셋을 기억해둬서, 다음 실행에서는 새 메시지만 처리한다.

파티셔닝 기준:
- 이벤트 발생 시각(event_time)이 아니라 "컨슘(수집)한 날짜" 기준으로 나눈다.
  나중에 도착한 이벤트 때문에 예전 파티션을 다시 여는 상황을 피하기 위함이다.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, from_json
from pyspark.sql.types import StringType, StructField, StructType

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "pubg-match-events"

PROJECT_ROOT = Path(__file__).parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "_checkpoints" / "bronze"

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("match_id", StringType(), nullable=False),
        StructField("event_time", StringType(), nullable=False),
        StructField("payload", StringType(), nullable=True),
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze-ingest")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    ).select("event.*", "kafka_partition", "kafka_offset", "kafka_timestamp")

    bronze = parsed.withColumn("ingestion_date", current_date())

    query = (
        bronze.writeStream.format("parquet")
        .option("path", str(BRONZE_DIR))
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .partitionBy("ingestion_date")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("Bronze 적재 완료")


if __name__ == "__main__":
    main()
