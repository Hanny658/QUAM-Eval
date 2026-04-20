from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TABLE_NAMES = (
    "users",
    "pois",
    "interactions",
    "categories",
    "poi_category_map",
    "social_edges",
    "source_mapping",
)


@dataclass
class AdaptResult:
    dataset: str
    tables: dict[str, list[dict[str, Any]]]
    consumed_files: list[str]


def empty_tables() -> dict[str, list[dict[str, Any]]]:
    return {name: [] for name in TABLE_NAMES}


def stable_id(dataset: str, entity: str, *parts: object) -> str:
    payload = "|".join("" if part is None else str(part) for part in parts)
    digest = hashlib.sha1(f"{dataset}|{entity}|{payload}".encode("utf-8")).hexdigest()[:20]
    return f"{dataset}_{entity}_{digest}"


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def isoformat_utc(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def isoformat_local(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def parse_iso_utc(raw: str) -> datetime:
    normalized = raw.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def dumps_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


_GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash_encode(latitude: float, longitude: float, precision: int = 7) -> str:
    lat_interval = [-90.0, 90.0]
    lon_interval = [-180.0, 180.0]
    bits = [16, 8, 4, 2, 1]
    bit = 0
    ch = 0
    geohash = []
    even = True

    while len(geohash) < precision:
        if even:
            mid = (lon_interval[0] + lon_interval[1]) / 2
            if longitude >= mid:
                ch |= bits[bit]
                lon_interval[0] = mid
            else:
                lon_interval[1] = mid
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2
            if latitude >= mid:
                ch |= bits[bit]
                lat_interval[0] = mid
            else:
                lat_interval[1] = mid

        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(_GEOHASH_BASE32[ch])
            bit = 0
            ch = 0
    return "".join(geohash)


def should_use_file(path: Path, selected_files: set[Path] | None) -> bool:
    if not selected_files:
        return True
    try:
        resolved = path.resolve()
    except FileNotFoundError:
        return False
    return resolved in selected_files


def register_source_mapping(
    tables: dict[str, list[dict[str, Any]]],
    dataset: str,
    entity_type: str,
    source_id: str | None,
    canonical_id: str,
) -> None:
    if not source_id:
        return
    tables["source_mapping"].append(
        {
            "dataset": dataset,
            "entity_type": entity_type,
            "source_id": source_id,
            "canonical_id": canonical_id,
        }
    )


def build_user_rows(
    dataset: str,
    source_user_ids: set[str],
    stats: dict[str, dict[str, Any]],
    profile_payloads: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    profile_payloads = profile_payloads or {}
    rows: list[dict[str, Any]] = []
    for source_user_id in sorted(source_user_ids):
        user_id = stable_id(dataset, "user", source_user_id)
        stat = stats.get(source_user_id, {})
        profile = profile_payloads.get(source_user_id, {})
        rows.append(
            {
                "user_id": user_id,
                "dataset": dataset,
                "source_user_id": source_user_id,
                "first_event_time_utc": isoformat_utc(stat.get("first_event_time_utc")),
                "last_event_time_utc": isoformat_utc(stat.get("last_event_time_utc")),
                "interaction_count": stat.get("interaction_count"),
                "active_days": stat.get("active_days"),
                "home_lat": stat.get("home_lat"),
                "home_lon": stat.get("home_lon"),
                "profile_json": dumps_json(profile) if profile else None,
            }
        )
    return rows


def update_user_stats(
    stats: dict[str, dict[str, Any]],
    source_user_id: str,
    event_time_utc: datetime,
    local_date: str | None,
    lat: float | None,
    lon: float | None,
) -> None:
    target = stats.setdefault(
        source_user_id,
        {
            "first_event_time_utc": event_time_utc,
            "last_event_time_utc": event_time_utc,
            "interaction_count": 0,
            "active_dates": set(),
            "_sum_lat": 0.0,
            "_sum_lon": 0.0,
            "_geo_count": 0,
        },
    )
    if event_time_utc < target["first_event_time_utc"]:
        target["first_event_time_utc"] = event_time_utc
    if event_time_utc > target["last_event_time_utc"]:
        target["last_event_time_utc"] = event_time_utc
    target["interaction_count"] += 1
    if local_date:
        target["active_dates"].add(local_date)
    if lat is not None and lon is not None:
        target["_sum_lat"] += lat
        target["_sum_lon"] += lon
        target["_geo_count"] += 1


def finalize_user_stats(stats: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    finalized: dict[str, dict[str, Any]] = {}
    for source_user_id, value in stats.items():
        geo_count = value["_geo_count"]
        finalized[source_user_id] = {
            "first_event_time_utc": value["first_event_time_utc"],
            "last_event_time_utc": value["last_event_time_utc"],
            "interaction_count": value["interaction_count"],
            "active_days": len(value["active_dates"]),
            "home_lat": (value["_sum_lat"] / geo_count) if geo_count else None,
            "home_lon": (value["_sum_lon"] / geo_count) if geo_count else None,
        }
    return finalized

