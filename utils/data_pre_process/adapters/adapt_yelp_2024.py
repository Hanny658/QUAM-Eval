from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .common import (
    AdaptResult,
    build_user_rows,
    dumps_json,
    empty_tables,
    finalize_user_stats,
    geohash_encode,
    isoformat_local,
    isoformat_utc,
    register_source_mapping,
    should_use_file,
    stable_id,
    update_user_stats,
)


DATASET_NAME = "yelp"
_BUSINESS_FILE = "yelp_academic_dataset_business.json"
_USER_FILE = "yelp_academic_dataset_user.json"
_REVIEW_FILE = "yelp_academic_dataset_review.json"
_TIP_FILE = "yelp_academic_dataset_tip.json"
_CHECKIN_FILE = "yelp_academic_dataset_checkin.json"
_SUPPORTED_FILES = (
    _BUSINESS_FILE,
    _USER_FILE,
    _REVIEW_FILE,
    _TIP_FILE,
    _CHECKIN_FILE,
)


def discover_input_files(raw_dataset_dir: Path) -> dict[str, Path]:
    file_map: dict[str, Path] = {}
    for name in _SUPPORTED_FILES:
        found = list(raw_dataset_dir.rglob(name))
        if found:
            file_map[name] = sorted(found, key=lambda p: len(p.parts))[0]
    return file_map


def _parse_local_datetime(raw: str) -> datetime | None:
    text = str(raw).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        try:
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def _split_categories(raw: Any) -> list[str]:
    if raw is None:
        return []
    text = str(raw).strip()
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def _extract_friends(raw: Any) -> list[str]:
    if raw is None:
        return []
    text = str(raw).strip()
    if not text or text.lower() == "none":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def adapt(
    raw_dataset_dir: Path,
    selected_files: set[Path] | None = None,
    limit: int | None = None,
    strict: bool = False,
) -> AdaptResult:
    files = discover_input_files(raw_dataset_dir)
    if strict and _BUSINESS_FILE not in files:
        raise FileNotFoundError(f"Missing required file: {_BUSINESS_FILE}")

    tables = empty_tables()
    consumed_files: list[str] = []

    source_user_ids: set[str] = set()
    source_poi_ids: set[str] = set()
    user_stats: dict[str, dict[str, Any]] = {}
    user_profiles: dict[str, dict[str, Any]] = {}

    poi_rows: dict[str, dict[str, Any]] = {}
    category_rows: dict[str, dict[str, Any]] = {}
    poi_category_seen: set[tuple[str, str]] = set()
    social_seen: set[tuple[str, str]] = set()

    # 1) Business -> POI + Category
    business_path = files.get(_BUSINESS_FILE)
    if business_path and should_use_file(business_path, selected_files):
        consumed_files.append(str(business_path.resolve()))
        with business_path.open("r", encoding="utf-8") as fin:
            for raw_line in fin:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                source_poi_id = str(obj.get("business_id", "")).strip()
                if not source_poi_id:
                    continue
                source_poi_ids.add(source_poi_id)
                poi_id = stable_id(DATASET_NAME, "poi", source_poi_id)

                lat = obj.get("latitude")
                lon = obj.get("longitude")
                try:
                    lat = float(lat) if lat is not None else None
                    lon = float(lon) if lon is not None else None
                except (TypeError, ValueError):
                    lat, lon = None, None

                poi_rows[source_poi_id] = {
                    "poi_id": poi_id,
                    "dataset": DATASET_NAME,
                    "source_poi_id": source_poi_id,
                    "name": obj.get("name"),
                    "lat": lat,
                    "lon": lon,
                    "geohash7": geohash_encode(lat, lon, precision=7) if lat is not None and lon is not None else None,
                    "city": obj.get("city"),
                    "state": obj.get("state"),
                    "country": None,
                    "timezone": None,
                    "is_active": bool(obj.get("is_open")) if obj.get("is_open") is not None else None,
                    "poi_meta_json": dumps_json(
                        {
                            "address": obj.get("address"),
                            "postal_code": obj.get("postal_code"),
                            "review_count": obj.get("review_count"),
                            "stars": obj.get("stars"),
                            "attributes": obj.get("attributes"),
                            "hours": obj.get("hours"),
                        }
                    ),
                }
                register_source_mapping(tables, DATASET_NAME, "poi", source_poi_id, poi_id)

                for idx, category_name in enumerate(_split_categories(obj.get("categories"))):
                    category_id = stable_id(DATASET_NAME, "category", category_name.lower())
                    if category_id not in category_rows:
                        category_rows[category_id] = {
                            "category_id": category_id,
                            "dataset": DATASET_NAME,
                            "source_category_id": None,
                            "name": category_name,
                            "parent_category_id": None,
                            "taxonomy_level": None,
                            "canonical_group": None,
                        }
                    key = (poi_id, category_id)
                    if key in poi_category_seen:
                        continue
                    poi_category_seen.add(key)
                    tables["poi_category_map"].append(
                        {
                            "dataset": DATASET_NAME,
                            "poi_id": poi_id,
                            "category_id": category_id,
                            "is_primary": idx == 0,
                            "confidence": 1.0,
                        }
                    )

    # 2) User -> profiles + social
    user_path = files.get(_USER_FILE)
    if user_path and should_use_file(user_path, selected_files):
        consumed_files.append(str(user_path.resolve()))
        with user_path.open("r", encoding="utf-8") as fin:
            for raw_line in fin:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                source_user_id = str(obj.get("user_id", "")).strip()
                if not source_user_id:
                    continue
                source_user_ids.add(source_user_id)
                user_profiles[source_user_id] = {
                    "name": obj.get("name"),
                    "review_count": obj.get("review_count"),
                    "average_stars": obj.get("average_stars"),
                    "fans": obj.get("fans"),
                    "yelping_since": obj.get("yelping_since"),
                }
                for friend_source_id in _extract_friends(obj.get("friends")):
                    if friend_source_id == source_user_id:
                        continue
                    ordered = tuple(sorted((source_user_id, friend_source_id)))
                    if ordered in social_seen:
                        continue
                    social_seen.add(ordered)
                    source_user_ids.update(ordered)
                    src_user_id = stable_id(DATASET_NAME, "user", ordered[0])
                    dst_user_id = stable_id(DATASET_NAME, "user", ordered[1])
                    source_edge_id = f"{ordered[0]}:{ordered[1]}"
                    social_edge_id = stable_id(DATASET_NAME, "social_edge", source_edge_id)
                    tables["social_edges"].append(
                        {
                            "social_edge_id": social_edge_id,
                            "dataset": DATASET_NAME,
                            "source_edge_id": source_edge_id,
                            "src_user_id": src_user_id,
                            "dst_user_id": dst_user_id,
                            "relation_type": "friend",
                            "is_directed": False,
                            "created_time_utc": None,
                            "edge_weight": 1.0,
                            "edge_meta_json": dumps_json({"raw_source": "user.friends"}),
                        }
                    )
                    register_source_mapping(
                        tables,
                        DATASET_NAME,
                        "social_edge",
                        source_edge_id,
                        social_edge_id,
                    )

    processed = 0

    # 3) Review -> Interaction
    review_path = files.get(_REVIEW_FILE)
    if review_path and should_use_file(review_path, selected_files):
        consumed_files.append(str(review_path.resolve()))
        with review_path.open("r", encoding="utf-8") as fin:
            for raw_line in fin:
                if limit is not None and processed >= limit:
                    break
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                source_interaction_id = str(obj.get("review_id", "")).strip()
                source_user_id = str(obj.get("user_id", "")).strip()
                source_poi_id = str(obj.get("business_id", "")).strip()
                local_dt = _parse_local_datetime(obj.get("date"))
                if not source_user_id or not source_poi_id or local_dt is None:
                    continue

                source_user_ids.add(source_user_id)
                source_poi_ids.add(source_poi_id)
                utc_dt = local_dt.replace(tzinfo=timezone.utc)
                user_id = stable_id(DATASET_NAME, "user", source_user_id)
                poi_id = stable_id(DATASET_NAME, "poi", source_poi_id)
                interaction_id = stable_id(
                    DATASET_NAME,
                    "interaction",
                    source_interaction_id or f"{source_user_id}:{source_poi_id}:{local_dt.isoformat()}",
                )

                poi = poi_rows.get(source_poi_id)
                lat = poi.get("lat") if poi else None
                lon = poi.get("lon") if poi else None
                local_date = local_dt.date().isoformat()
                update_user_stats(user_stats, source_user_id, utc_dt, local_date, lat, lon)

                tables["interactions"].append(
                    {
                        "interaction_id": interaction_id,
                        "dataset": DATASET_NAME,
                        "source_interaction_id": source_interaction_id or None,
                        "user_id": user_id,
                        "poi_id": poi_id,
                        "event_time_utc": isoformat_utc(utc_dt),
                        "event_time_local": isoformat_local(local_dt),
                        "timezone_offset_min": None,
                        "local_date": local_date,
                        "dow_local": local_dt.weekday(),
                        "hour_local": local_dt.hour,
                        "event_type": "review",
                        "text": obj.get("text"),
                        "rating": float(obj["stars"]) if obj.get("stars") is not None else None,
                        "geo_distance_prev_km": None,
                        "delta_time_prev_min": None,
                        "interaction_meta_json": dumps_json(
                            {"useful": obj.get("useful"), "funny": obj.get("funny"), "cool": obj.get("cool")}
                        ),
                    }
                )
                register_source_mapping(
                    tables,
                    DATASET_NAME,
                    "interaction",
                    source_interaction_id or interaction_id,
                    interaction_id,
                )
                processed += 1

    # 4) Tip -> Interaction
    tip_path = files.get(_TIP_FILE)
    if tip_path and should_use_file(tip_path, selected_files):
        consumed_files.append(str(tip_path.resolve()))
        with tip_path.open("r", encoding="utf-8") as fin:
            for raw_line in fin:
                if limit is not None and processed >= limit:
                    break
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                source_user_id = str(obj.get("user_id", "")).strip()
                source_poi_id = str(obj.get("business_id", "")).strip()
                local_dt = _parse_local_datetime(obj.get("date"))
                if not source_user_id or not source_poi_id or local_dt is None:
                    continue

                source_user_ids.add(source_user_id)
                source_poi_ids.add(source_poi_id)
                utc_dt = local_dt.replace(tzinfo=timezone.utc)
                user_id = stable_id(DATASET_NAME, "user", source_user_id)
                poi_id = stable_id(DATASET_NAME, "poi", source_poi_id)
                source_interaction_id = f"{source_user_id}:{source_poi_id}:{local_dt.isoformat()}"
                interaction_id = stable_id(DATASET_NAME, "interaction", source_interaction_id)

                poi = poi_rows.get(source_poi_id)
                lat = poi.get("lat") if poi else None
                lon = poi.get("lon") if poi else None
                local_date = local_dt.date().isoformat()
                update_user_stats(user_stats, source_user_id, utc_dt, local_date, lat, lon)

                tables["interactions"].append(
                    {
                        "interaction_id": interaction_id,
                        "dataset": DATASET_NAME,
                        "source_interaction_id": source_interaction_id,
                        "user_id": user_id,
                        "poi_id": poi_id,
                        "event_time_utc": isoformat_utc(utc_dt),
                        "event_time_local": isoformat_local(local_dt),
                        "timezone_offset_min": None,
                        "local_date": local_date,
                        "dow_local": local_dt.weekday(),
                        "hour_local": local_dt.hour,
                        "event_type": "tip",
                        "text": obj.get("text"),
                        "rating": None,
                        "geo_distance_prev_km": None,
                        "delta_time_prev_min": None,
                        "interaction_meta_json": dumps_json({"compliment_count": obj.get("compliment_count")}),
                    }
                )
                register_source_mapping(
                    tables,
                    DATASET_NAME,
                    "interaction",
                    source_interaction_id,
                    interaction_id,
                )
                processed += 1

    # 5) Checkin -> Interaction
    checkin_path = files.get(_CHECKIN_FILE)
    if checkin_path and should_use_file(checkin_path, selected_files):
        consumed_files.append(str(checkin_path.resolve()))
        with checkin_path.open("r", encoding="utf-8") as fin:
            for raw_line in fin:
                if limit is not None and processed >= limit:
                    break
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                source_poi_id = str(obj.get("business_id", "")).strip()
                if not source_poi_id:
                    continue
                source_poi_ids.add(source_poi_id)
                dates_text = str(obj.get("date", "")).strip()
                if not dates_text:
                    continue
                for date_part in [part.strip() for part in dates_text.split(",") if part.strip()]:
                    if limit is not None and processed >= limit:
                        break
                    local_dt = _parse_local_datetime(date_part)
                    if local_dt is None:
                        continue
                    utc_dt = local_dt.replace(tzinfo=timezone.utc)
                    poi_id = stable_id(DATASET_NAME, "poi", source_poi_id)
                    source_interaction_id = f"checkin:{source_poi_id}:{local_dt.isoformat()}"
                    interaction_id = stable_id(DATASET_NAME, "interaction", source_interaction_id)

                    tables["interactions"].append(
                        {
                            "interaction_id": interaction_id,
                            "dataset": DATASET_NAME,
                            "source_interaction_id": source_interaction_id,
                            "user_id": None,
                            "poi_id": poi_id,
                            "event_time_utc": isoformat_utc(utc_dt),
                            "event_time_local": isoformat_local(local_dt),
                            "timezone_offset_min": None,
                            "local_date": local_dt.date().isoformat(),
                            "dow_local": local_dt.weekday(),
                            "hour_local": local_dt.hour,
                            "event_type": "checkin",
                            "text": None,
                            "rating": None,
                            "geo_distance_prev_km": None,
                            "delta_time_prev_min": None,
                            "interaction_meta_json": dumps_json({"aggregated_checkin": True}),
                        }
                    )
                    register_source_mapping(
                        tables,
                        DATASET_NAME,
                        "interaction",
                        source_interaction_id,
                        interaction_id,
                    )
                    processed += 1

    finalized_stats = finalize_user_stats(user_stats)
    tables["users"] = build_user_rows(DATASET_NAME, source_user_ids, finalized_stats, user_profiles)
    for row in tables["users"]:
        register_source_mapping(tables, DATASET_NAME, "user", row["source_user_id"], row["user_id"])

    # Backfill POI shells for interactions referencing missing business rows.
    for source_poi_id in sorted(source_poi_ids):
        if source_poi_id in poi_rows:
            continue
        poi_id = stable_id(DATASET_NAME, "poi", source_poi_id)
        poi_rows[source_poi_id] = {
            "poi_id": poi_id,
            "dataset": DATASET_NAME,
            "source_poi_id": source_poi_id,
            "name": None,
            "lat": None,
            "lon": None,
            "geohash7": None,
            "city": None,
            "state": None,
            "country": None,
            "timezone": None,
            "is_active": None,
            "poi_meta_json": dumps_json({"backfilled": True}),
        }
        register_source_mapping(tables, DATASET_NAME, "poi", source_poi_id, poi_id)

    tables["pois"] = sorted(poi_rows.values(), key=lambda row: row["poi_id"])
    tables["categories"] = sorted(category_rows.values(), key=lambda row: row["category_id"])

    return AdaptResult(dataset=DATASET_NAME, tables=tables, consumed_files=consumed_files)
