"""
합성 게임 매치 이벤트 생성기

목적:
- PUBG 공식 텔레메트리 스펙(scripts/inspect_pubg_telemetry.py로 확인함)을 참고해
  설계한 이벤트 타입으로, 실제 매치처럼 보이는 이벤트 시퀀스를 대량 생성한다.
- 실제 API처럼 특정 플레이어를 대상으로 폴링하지 않고 직접 생성하는 이유:
  PUBG API는 특정 플레이어/매치ID 단위로만 조회 가능해서 전 세계 매치를 다
  가져오는 글로벌 피드가 없다. 그래서 원하는 규모의 데이터를 안정적으로
  확보하려면 합성 생성이 더 낫다.

출력:
- data/raw/events/YYYY-MM-DD.json: 하루 동안 시작된 매치들의 이벤트 (날짜별
  파티셔닝, generate_logs.py와 동일한 패턴)
- data/raw/player_profile_seasons.json: 시즌별 플레이어 티어 스냅샷
  (매치 이벤트와 무관한 별도 축 — Silver에서 SCD Type 2로 병합할 원본)

의도적으로 반영한 지저분한 데이터 (Silver 정제 연습용):
- 일부 이벤트의 event_id 중복 (네트워크 재전송 재현)
- 일부 매치는 LogMatchEnd 누락 (매치 로그 유실 재현)
- 일부 이벤트에서 account_id 결측 (비정상 종료/봇 등 재현)
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

NUM_DAYS = 7
MATCHES_PER_DAY = 150
PLAYER_POOL_SIZE = 300

MAPS = ["Erangel", "Miramar", "Sanhok", "Vikendi"]
MODES = [("solo", 1), ("duo", 2), ("squad", 4)]

WEAPONS = ["WeapM416_C", "WeapAK47_C", "WeapKar98k_C", "WeapUMP_C", "WeapVSS_C"]
HEAL_ITEMS = ["Item_Heal_FirstAid_C", "Item_Heal_MedKit_C", "Item_Boost_EnergyDrink_C"]
ARMOR_ITEMS = ["Item_Armor_Level2_C", "Item_Armor_Level3_C"]
AMMO_ITEMS = ["Item_Ammo_556mm_C", "Item_Ammo_762mm_C"]

DUPLICATE_EVENT_RATE = 0.01
MATCH_END_MISSING_RATE = 0.05
MISSING_ACCOUNT_ID_RATE = 0.03

SEASONS = [
    ("2026-S1", datetime(2026, 1, 15)),
    ("2026-S2", datetime(2026, 4, 15)),
    ("2026-S3", datetime(2026, 7, 15)),
]
TIERS = ["Bronze", "Silver", "Gold", "Platinum", "Diamond", "Master"]

OUTPUT_DIR = Path(__file__).parent / "data" / "raw"


def build_player_pool(n: int) -> list[dict]:
    return [
        {"account_id": f"account.{uuid.uuid4().hex}", "nickname": f"player_{i:04d}"}
        for i in range(n)
    ]


def make_event(event_type: str, match_id: str, event_time: datetime, payload: dict) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "match_id": match_id,
        "event_time": event_time.isoformat(),
        "payload": json.dumps(payload, ensure_ascii=False),
    }


def generate_match(match_start: datetime, player_pool: list[dict]) -> list[dict]:
    mode, team_size = random.choice(MODES)
    num_participants = random.randint(4, min(60, len(player_pool)))
    participants = random.sample(player_pool, num_participants)

    match_id = str(uuid.uuid4())
    duration = timedelta(minutes=random.randint(18, 32))
    match_end = match_start + duration

    events = [
        make_event(
            "LogMatchStart",
            match_id,
            match_start,
            {"map_name": random.choice(MAPS), "mode": mode, "team_size": team_size},
        )
    ]

    alive = list(participants)
    survivors = random.randint(1, min(3, num_participants))
    num_kills = num_participants - survivors

    for _ in range(num_kills):
        if len(alive) <= survivors:
            break
        attacker, victim = random.sample(alive, 2)
        event_time = match_start + (match_end - match_start) * random.random()

        events.append(
            make_event(
                "LogPlayerTakeDamage",
                match_id,
                event_time,
                {
                    "attacker_account_id": attacker["account_id"],
                    "victim_account_id": victim["account_id"],
                    "damage": round(random.uniform(20, 100), 1),
                    "damage_type": "Gun",
                },
            )
        )
        events.append(
            make_event(
                "LogPlayerKillV2",
                match_id,
                event_time + timedelta(seconds=1),
                {
                    "attacker_account_id": attacker["account_id"],
                    "attacker_nickname": attacker["nickname"],
                    "victim_account_id": victim["account_id"],
                    "victim_nickname": victim["nickname"],
                    "weapon": random.choice(WEAPONS),
                    "distance": round(random.uniform(5, 300), 1),
                },
            )
        )
        alive.remove(victim)

    # 죽지 않는 논데미지성 데미지 이벤트 (교전했지만 안 죽은 상황)
    for _ in range(random.randint(0, num_participants)):
        if len(participants) < 2:
            break
        attacker, victim = random.sample(participants, 2)
        event_time = match_start + (match_end - match_start) * random.random()
        events.append(
            make_event(
                "LogPlayerTakeDamage",
                match_id,
                event_time,
                {
                    "attacker_account_id": attacker["account_id"],
                    "victim_account_id": victim["account_id"],
                    "damage": round(random.uniform(5, 40), 1),
                    "damage_type": "Gun",
                },
            )
        )

    # 아이템 습득
    for player in participants:
        for _ in range(random.randint(1, 5)):
            item_id = random.choice(WEAPONS + HEAL_ITEMS + ARMOR_ITEMS + AMMO_ITEMS)
            category = (
                "Weapon" if item_id in WEAPONS
                else "Healing" if item_id in HEAL_ITEMS
                else "Armor" if item_id in ARMOR_ITEMS
                else "Ammo"
            )
            event_time = match_start + (match_end - match_start) * random.random()
            events.append(
                make_event(
                    "LogItemPickup",
                    match_id,
                    event_time,
                    {
                        "account_id": player["account_id"],
                        "nickname": player["nickname"],
                        "item_id": item_id,
                        "category": category,
                    },
                )
            )

    if random.random() >= MATCH_END_MISSING_RATE:
        events.append(
            make_event(
                "LogMatchEnd",
                match_id,
                match_end,
                {"duration_seconds": int(duration.total_seconds()), "survivor_count": len(alive)},
            )
        )

    return events


def apply_dirty_data(events: list[dict]) -> list[dict]:
    # 1) 일부 이벤트 account_id 결측 (payload를 다시 파싱해서 지움)
    for event in events:
        if event["event_type"] in ("LogMatchStart", "LogMatchEnd"):
            continue
        if random.random() < MISSING_ACCOUNT_ID_RATE:
            payload = json.loads(event["payload"])
            for key in ("account_id", "attacker_account_id", "victim_account_id"):
                if key in payload:
                    payload[key] = None
            event["payload"] = json.dumps(payload, ensure_ascii=False)

    # 2) 일부 이벤트 중복 삽입 (네트워크 재전송 재현)
    duplicate_candidates = [e for e in events if e["event_type"] not in ("LogMatchStart", "LogMatchEnd")]
    num_duplicates = int(len(duplicate_candidates) * DUPLICATE_EVENT_RATE)
    if num_duplicates:
        duplicates = random.sample(duplicate_candidates, num_duplicates)
        events = events + duplicates

    random.shuffle(events)
    return events


def generate_player_profile_seasons(player_pool: list[dict]) -> list[dict]:
    snapshots = []
    for player in player_pool:
        tier_index = random.randint(0, len(TIERS) - 1)
        for season_name, snapshot_date in SEASONS:
            tier_index = max(0, min(len(TIERS) - 1, tier_index + random.choice([-1, 0, 0, 1])))
            snapshots.append(
                {
                    "account_id": player["account_id"],
                    "nickname": player["nickname"],
                    "season": season_name,
                    "tier": TIERS[tier_index],
                    "snapshot_date": snapshot_date.isoformat(),
                }
            )
    return snapshots


def main():
    events_dir = OUTPUT_DIR / "events"
    events_dir.mkdir(parents=True, exist_ok=True)

    player_pool = build_player_pool(PLAYER_POOL_SIZE)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    for i in range(NUM_DAYS):
        date = today - timedelta(days=i)
        day_events = []
        for _ in range(MATCHES_PER_DAY):
            match_start = date + timedelta(seconds=random.randint(0, 86399))
            day_events.extend(generate_match(match_start, player_pool))

        day_events = apply_dirty_data(day_events)

        file_path = events_dir / f"{date.strftime('%Y-%m-%d')}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(day_events, f, ensure_ascii=False, indent=2)

        print(f"생성 완료: {file_path} ({len(day_events)}건)")

    profile_path = OUTPUT_DIR / "player_profile_seasons.json"
    profiles = generate_player_profile_seasons(player_pool)
    with open(profile_path, "w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)
    print(f"생성 완료: {profile_path} ({len(profiles)}건)")


if __name__ == "__main__":
    main()
