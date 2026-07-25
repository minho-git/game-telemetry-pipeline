"""
Kafka Producer — 합성 매치 이벤트를 Kafka topic으로 전송

목적:
- data/raw/events/*.json에 저장된 이벤트를 읽어서 Kafka topic(pubg-match-events)으로
  전송한다.
- player_profile_seasons.json은 여기서 다루지 않는다: 이건 실시간 이벤트 스트림이
  아니라 천천히 바뀌는 차원(dimension) 참조 데이터라, Kafka를 거치지 않고 Silver에서
  직접 배치로 읽는다 (팩트성 스트림과 차원 참조 데이터는 적재 경로가 다른 경우가 많다).

메시지 키 설계:
- key=match_id. 같은 매치의 이벤트는 항상 같은 파티션으로 가서, 파티션이 여러 개로
  늘어나도 매치 내 이벤트 순서가 보장된다.

전송 속도:
- 하루치가 2.5만~2.8만 건이라 건당 딜레이를 주지 않고 빠르게 전송한다. 이 볼륨에서는
  "실시간처럼 보이기"보다 "이미 있는 대량 데이터를 빠르게 적재"가 더 현실적인
  시나리오다.
"""

import json
import time
from pathlib import Path

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "pubg-match-events"
EVENTS_DIR = Path(__file__).parent / "data" / "raw" / "events"


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )


def main():
    files = sorted(EVENTS_DIR.glob("*.json"))
    if not files:
        print(f"전송할 이벤트 파일이 없습니다: {EVENTS_DIR}")
        return

    producer = build_producer()
    total_sent = 0
    start = time.time()

    for file_path in files:
        with open(file_path, encoding="utf-8") as f:
            events = json.load(f)

        for event in events:
            producer.send(TOPIC, key=event["match_id"], value=event)
            total_sent += 1
            if total_sent % 10000 == 0:
                print(f"  {total_sent}건 전송 중...")

        print(f"전송 완료: {file_path.name} ({len(events)}건)")

    producer.flush()
    producer.close()
    elapsed = time.time() - start
    print(f"총 {total_sent}건 전송 완료 -> topic '{TOPIC}' ({elapsed:.1f}초)")


if __name__ == "__main__":
    main()
