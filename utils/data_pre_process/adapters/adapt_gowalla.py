from __future__ import annotations

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
    parse_iso_utc,
    register_source_mapping,
    should_use_file,
    stable_id,
    update_user_stats,
)


DATASET_NAME = "gowalla"
_CHECKINS_FILE = "loc-gowalla_totalCheckins.txt"
_EDGES_FILE = "loc-gowalla_edges.txt"


def discover_input_files(raw_dataset_dir: Path) -> dict[str, Path]:
    file_map: dict[str, Path] = {}
    checkins = list(raw_dataset_dir.rglob(_CHECKINS_FILE))
    edges = list(raw_dataset_dir.rglob(_EDGES_FILE))
    if checkins:
        file_map[_CHECKINS_FILE] = sorted(checkins, key=lambda p: len(p.parts))[0]
    if edges:
        file_map[_EDGES_FILE] = sorted(edges, key=lambda p: len(p.parts))[0]
    return file_map


def adapt(
    raw_dataset_dir: Path,
    selected_files: set[Path] | None = None,
    limit: int | None = None,
    strict: bool = False,
) -> AdaptResult:
    files = discover_input_files(raw_dataset_dir)
    if strict and _CHECKINS_FILE not in files:
        raise FileNotFoundError(f"Missing required file: {_CHECKINS_FILE}")

    tables = empty_tables()
    consumed_files: list[str] = []

    poi_rows: dict[str, dict[str, Any]] = {}
    source_user_ids: set[str] = set()
    user_stats: dict[str, dict[str, Any]] = {}
    social_seen: set[tuple[str, str]] = set()
    interaction_seq = 0
    processed = 0

    checkins_path = files.get(_CHECKINS_FILE)
    if checkins_path and should_use_file(checkins_path, selected_files):
        consumed_files.append(str(checkins_path.resolve()))
        with checkins_path.open("r", encoding="utf-8") as fin:
            for raw_line in fin:
                if limit is not None and processed >= limit:
                    break
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) != 5:
                    continue
                source_user_id = parts[0].strip()
                timestamp = parts[1].strip()
                source_poi_id = parts[4].strip()

                try:
                    lat = float(parts[2].strip())
                    lon = float(parts[3].strip())
                    utc_dt = parse_iso_utc(timestamp)
                except (TypeError, ValueError):
                    continue

                user_id = stable_id(DATASET_NAME, "user", source_user_id)
                poi_id = stable_id(DATASET_NAME, "poi", source_poi_id)
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
                local_dt = utc_dt
                local_date = local_dt.date().isoformat()
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
                        "city": None,
                        "state": None,
                        "country": None,
                        "timezone": None,
                        "is_active": None,
                        "poi_meta_json": None,
                    }
                    register_source_mapping(tables, DATASET_NAME, "poi", source_poi_id, poi_id)

                tables["interactions"].append(
                    {
                        "interaction_id": interaction_id,
                        "dataset": DATASET_NAME,
                        "source_interaction_id": None,
                        "user_id": user_id,
                        "poi_id": poi_id,
                        "event_time_utc": isoformat_utc(utc_dt),
                        "event_time_local": isoformat_local(local_dt),
                        "timezone_offset_min": 0,
                        "local_date": local_date,
                        "dow_local": local_dt.weekday(),
                        "hour_local": local_dt.hour,
                        "event_type": "checkin",
                        "text": None,
                        "rating": None,
                        "geo_distance_prev_km": None,
                        "delta_time_prev_min": None,
                        "interaction_meta_json": None,
                    }
                )
                processed += 1

    edges_path = files.get(_EDGES_FILE)
    if edges_path and should_use_file(edges_path, selected_files):
        consumed_files.append(str(edges_path.resolve()))
        edge_idx = 0
        with edges_path.open("r", encoding="utf-8") as fin:
            for raw_line in fin:
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) != 2:
                    continue
                src_raw = parts[0].strip()
                dst_raw = parts[1].strip()
                if not src_raw or not dst_raw or src_raw == dst_raw:
                    continue
                ordered = tuple(sorted((src_raw, dst_raw)))
                if ordered in social_seen:
                    continue
                social_seen.add(ordered)
                source_user_ids.add(src_raw)
                source_user_ids.add(dst_raw)

                src_user_id = stable_id(DATASET_NAME, "user", ordered[0])
                dst_user_id = stable_id(DATASET_NAME, "user", ordered[1])
                source_edge_id = f"{ordered[0]}:{ordered[1]}"
                social_edge_id = stable_id(DATASET_NAME, "social_edge", source_edge_id, edge_idx)
                edge_idx += 1

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
                        "edge_meta_json": dumps_json({"raw_source": "gowalla_edges"}),
                    }
                )
                register_source_mapping(
                    tables,
                    DATASET_NAME,
                    "social_edge",
                    source_edge_id,
                    social_edge_id,
                )

    finalized_stats = finalize_user_stats(user_stats)
    tables["users"] = build_user_rows(DATASET_NAME, source_user_ids, finalized_stats)
    for row in tables["users"]:
        register_source_mapping(
            tables,
            DATASET_NAME,
            "user",
            row["source_user_id"],
            row["user_id"],
        )

    tables["pois"] = sorted(poi_rows.values(), key=lambda row: row["poi_id"])
    tables["categories"] = []
    tables["poi_category_map"] = []

    return AdaptResult(dataset=DATASET_NAME, tables=tables, consumed_files=consumed_files)

