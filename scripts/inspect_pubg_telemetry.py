"""
일회성 조사 스크립트: 합성 데이터 스키마를 설계하기 전에 PUBG 공식 API로
실제 텔레메트리 구조(이벤트 타입, 필드)를 확인하기 위한 것.
파이프라인의 일부가 아니라 스키마 설계 근거 자료.
"""

import gzip
import json
import os
from collections import Counter
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.environ["PUBG_API_KEY"].strip()
SHARD = "steam"
BASE_URL = "https://api.pubg.com"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/vnd.api+json",
}


def get_sample_match_ids() -> list:
    resp = requests.get(f"{BASE_URL}/shards/{SHARD}/samples", headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    match_refs = data["data"]["relationships"]["matches"]["data"]
    return [m["id"] for m in match_refs]


def get_telemetry_url(match_id: str) -> str:
    resp = requests.get(f"{BASE_URL}/shards/{SHARD}/matches/{match_id}", headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    for item in data["included"]:
        if item["type"] == "asset":
            return item["attributes"]["URL"]
    raise ValueError("telemetry asset not found")


def get_telemetry(url: str) -> list:
    resp = requests.get(url)
    resp.raise_for_status()
    raw = resp.content
    try:
        raw = gzip.decompress(raw)
    except OSError:
        pass  # 이미 압축 해제된 상태일 수 있음
    return json.loads(raw)


def main():
    match_ids = get_sample_match_ids()
    print(f"샘플 매치 수: {len(match_ids)}")

    events = None
    match_id = None
    for candidate_id in match_ids[:8]:  # 분당 요청 제한(10회) 감안해 앞 8개만
        telemetry_url = get_telemetry_url(candidate_id)
        candidate_events = get_telemetry(telemetry_url)
        type_counts = Counter(e["_T"] for e in candidate_events)
        if type_counts.get("LogPlayerKillV2", 0) > 0:
            match_id = candidate_id
            events = candidate_events
            break
        print(f"  {candidate_id}: 킬 이벤트 없음, 스킵")

    if events is None:
        print("킬 이벤트가 있는 매치를 못 찾음")
        return

    print(f"\n선택된 match_id: {match_id}")
    print(f"총 이벤트 수: {len(events)}")

    type_counts = Counter(e["_T"] for e in events)
    print("\n이벤트 타입별 개수:")
    for event_type, count in type_counts.most_common():
        print(f"  {event_type}: {count}")

    interesting = ["LogMatchStart", "LogMatchEnd", "LogPlayerKillV2", "LogPlayerTakeDamage", "LogItemPickup"]
    print("\n샘플 이벤트:")
    for event_type in interesting:
        sample = next((e for e in events if e["_T"] == event_type), None)
        if sample:
            print(f"\n--- {event_type} ---")
            print(json.dumps(sample, indent=2, ensure_ascii=False)[:2000])


if __name__ == "__main__":
    main()
