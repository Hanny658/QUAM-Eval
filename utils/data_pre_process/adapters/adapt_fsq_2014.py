from __future__ import annotations

from datetime import datetime, timedelta
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


DATASET_NAME = "foursquare_classic"
_SUPPORTED_FILES = ("dataset_TSMC2014_NYC.txt", "dataset_TSMC2014_TKY.txt")


def discover_input_files(raw_dataset_dir: Path) -> dict[str, Path]:
    discovered: dict[str, list[Path]] = {name: [] for name in _SUPPORTED_FILES}
    for name in _SUPPORTED_FILES:
        discovered[name].extend(raw_dataset_dir.rglob(name))

    selected: dict[str, Path] = {}
    for name, candidates in discovered.items():
        if not candidates:
            continue
        # Prefer shallow path to avoid duplicated nested extraction copies.
        selected[name] = sorted(candidates, key=lambda p: len(p.parts))[0]
    return selected


def adapt(
    raw_dataset_dir: Path,
    selected_files: set[Path] | None = None,
    limit: int | None = None,
    strict: bool = False,
) -> AdaptResult:
    files = discover_input_files(raw_dataset_dir)
    if strict and not files:
        raise FileNotFoundError(f"No supported files found under: {raw_dataset_dir}")

    tables = empty_tables()
    consumed_files: list[str] = []

    poi_rows: dict[str, dict[str, Any]] = {}
    category_rows: dict[str, dict[str, Any]] = {}
    poi_category_seen: set[tuple[str, str]] = set()
    source_user_ids: set[str] = set()
    user_stats: dict[str, dict[str, Any]] = {}

    interaction_seq = 0
    processed = 0

    for file_name, path in files.items():
        if not should_use_file(path, selected_files):
            continue
        consumed_files.append(str(path.resolve()))

        split_tag = "NYC" if "NYC" in file_name else "TKY"
        with path.open("r", encoding="utf-8", errors="replace") as fin:
            for raw_line in fin:
                if limit is not None and processed >= limit:
                    break
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) != 8:
                    continue

                source_user_id = parts[0].strip()
                source_poi_id = parts[1].strip()
                source_category_id = parts[2].strip()
                category_name = parts[3].strip()
                try:
                    lat = float(parts[4].strip())
                    lon = float(parts[5].strip())
                    timezone_offset_min = int(parts[6].strip())
                    utc_dt = datetime.strptime(parts[7].strip(), "%a %b %d %H:%M:%S %z %Y")
                except (ValueError, TypeError):
                    continue

                local_dt = utc_dt + timedelta(minutes=timezone_offset_min)
                local_date = local_dt.date().isoformat()

                user_id = stable_id(DATASET_NAME, "user", source_user_id)
                poi_id = stable_id(DATASET_NAME, "poi", source_poi_id)
                category_id = stable_id(DATASET_NAME, "category", source_category_id)
                interaction_id = stable_id(
                    DATASET_NAME,
                    "interaction",
                    source_user_id,
                    source_poi_id,
                    isoformat_utc(utc_dt),
                    interaction_seq,
                )
                interaction_seq += 1

                source_user_ids.add(source_user_id)
                update_user_stats(user_stats, source_user_id, utc_dt, local_date, lat, lon)

                if source_poi_id not in poi_rows:
                    poi_rows[source_poi_id] = {
                        "poi_id": poi_id,
                        "dataset": DATASET_NAME,
                        "source_poi_id": source_poi_id,
                        "name": None,
                        "lat": lat,
                        "lon": lon,
                        "geohash7": geohash_encode(lat, lon, precision=7),
                        "city": split_tag,
                        "state": None,
                        "country": None,
                        "timezone": None,
                        "is_active": None,
                        "poi_meta_json": dumps_json({"split": split_tag}),
                    }
                    register_source_mapping(tables, DATASET_NAME, "poi", source_poi_id, poi_id)

                if source_category_id and source_category_id not in category_rows:
                    category_rows[source_category_id] = {
                        "category_id": category_id,
                        "dataset": DATASET_NAME,
                        "source_category_id": source_category_id,
                        "name": category_name or source_category_id,
                        "parent_category_id": None,
                        "taxonomy_level": None,
                        "canonical_group": None,
                    }
                    register_source_mapping(
                        tables,
                        DATASET_NAME,
                        "category",
                        source_category_id,
                        category_id,
                    )

                if source_category_id:
                    key = (poi_id, category_id)
                    if key not in poi_category_seen:
                        poi_category_seen.add(key)
                        tables["poi_category_map"].append(
                            {
                                "dataset": DATASET_NAME,
                                "poi_id": poi_id,
                                "category_id": category_id,
                                "is_primary": True,
                                "confidence": 1.0,
                            }
                        )

                tables["interactions"].append(
                    {
                        "interaction_id": interaction_id,
                        "dataset": DATASET_NAME,
                        "source_interaction_id": None,
                        "user_id": user_id,
                        "poi_id": poi_id,
                        "event_time_utc": isoformat_utc(utc_dt),
                        "event_time_local": isoformat_local(local_dt),
                        "timezone_offset_min": timezone_offset_min,
                        "local_date": local_date,
                        "dow_local": local_dt.weekday(),
                        "hour_local": local_dt.hour,
                        "event_type": "checkin",
                        "text": None,
                        "rating": None,
                        "geo_distance_prev_km": None,
                        "delta_time_prev_min": None,
                        "interaction_meta_json": dumps_json({"split": split_tag}),
                    }
                )
                processed += 1

    finalized_stats = finalize_user_stats(user_stats)
    tables["users"] = build_user_rows(DATASET_NAME, source_user_ids, finalized_stats)
    tables["pois"] = sorted(poi_rows.values(), key=lambda row: row["poi_id"])
    tables["categories"] = sorted(category_rows.values(), key=lambda row: row["category_id"])
    tables["social_edges"] = []

    for row in tables["users"]:
        register_source_mapping(
            tables,
            DATASET_NAME,
            "user",
            row["source_user_id"],
            row["user_id"],
        )

    return AdaptResult(dataset=DATASET_NAME, tables=tables, consumed_files=consumed_files)
