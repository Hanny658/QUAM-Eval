from __future__ import annotations

import hashlib
import json
import math
import statistics
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
_EARTH_RADIUS_KM = 6371.0088


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


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    data = sorted(values)
    if len(data) == 1:
        return data[0]
    pos = (len(data) - 1) * q
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return data[lower]
    weight = pos - lower
    return data[lower] * (1.0 - weight) + data[upper] * weight


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad, lon1_rad = math.radians(lat1), math.radians(lon1)
    lat2_rad, lon2_rad = math.radians(lat2), math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return _EARTH_RADIUS_KM * c


def _parse_profile_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _time_band(hour_value: int) -> str:
    if 0 <= hour_value <= 5:
        return "night"
    if 6 <= hour_value <= 11:
        return "morning"
    if 12 <= hour_value <= 17:
        return "afternoon"
    return "evening"


def _category_distribution(
    events: list[dict[str, Any]],
    topk_categories: int,
) -> dict[str, Any]:
    category_counts: dict[str, int] = {}
    for event in events:
        for category_id in event["category_ids"]:
            category_counts[category_id] = category_counts.get(category_id, 0) + 1
    total = sum(category_counts.values())
    if total <= 0:
        return {"topk_probs": [], "entropy": None, "observed_category_events": 0}

    pairs = sorted(category_counts.items(), key=lambda item: (-item[1], item[0]))
    topk = []
    entropy = 0.0
    for category_id, count in pairs:
        prob = count / total
        if prob > 0:
            entropy -= prob * math.log(prob)
        if len(topk) < topk_categories:
            topk.append({"category_id": category_id, "prob": prob})
    return {
        "topk_probs": topk,
        "entropy": entropy,
        "observed_category_events": total,
    }


def _temporal_distribution(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {"hour_hist_24": [], "dow_hist_7": [], "weekend_ratio": None}
    hour_counts = [0] * 24
    dow_counts = [0] * 7
    for event in events:
        hour_counts[event["hour_local"]] += 1
        dow_counts[event["dow_local"]] += 1
    total = len(events)
    weekend = dow_counts[5] + dow_counts[6]
    return {
        "hour_hist_24": [count / total for count in hour_counts],
        "dow_hist_7": [count / total for count in dow_counts],
        "weekend_ratio": weekend / total,
    }


def _spatial_radius(
    events: list[dict[str, Any]],
    home_lat: float | None,
    home_lon: float | None,
    poi_latlon: dict[str, tuple[float | None, float | None]],
) -> dict[str, Any]:
    if home_lat is None or home_lon is None:
        return {"radius_of_gyration_km": None, "p50_km": None, "p90_km": None}

    distances = []
    for event in events:
        poi_coords = poi_latlon.get(event["poi_id"])
        if not poi_coords:
            continue
        poi_lat, poi_lon = poi_coords
        if poi_lat is None or poi_lon is None:
            continue
        distances.append(_haversine_km(home_lat, home_lon, poi_lat, poi_lon))

    if not distances:
        return {"radius_of_gyration_km": None, "p50_km": None, "p90_km": None}

    radius_of_gyration = math.sqrt(sum(d * d for d in distances) / len(distances))
    return {
        "radius_of_gyration_km": radius_of_gyration,
        "p50_km": _quantile(distances, 0.5),
        "p90_km": _quantile(distances, 0.9),
    }


def _popularity_percentile_map(interactions: list[dict[str, Any]]) -> dict[str, float]:
    poi_counts: dict[str, int] = {}
    for row in interactions:
        user_id = row.get("user_id")
        poi_id = row.get("poi_id")
        if not user_id or not poi_id:
            continue
        poi_counts[poi_id] = poi_counts.get(poi_id, 0) + 1

    if not poi_counts:
        return {}

    sorted_items = sorted(poi_counts.items(), key=lambda item: (item[1], item[0]))
    n = len(sorted_items)
    if n == 1:
        poi_id = sorted_items[0][0]
        return {poi_id: 1.0}

    count_to_positions: dict[int, list[int]] = {}
    for idx, (_, count) in enumerate(sorted_items):
        count_to_positions.setdefault(count, []).append(idx)
    count_to_percentile = {
        count: (sum(positions) / len(positions)) / (n - 1)
        for count, positions in count_to_positions.items()
    }
    return {
        poi_id: count_to_percentile[count]
        for poi_id, count in sorted_items
    }


def _yelp_review_style(events: list[dict[str, Any]]) -> dict[str, Any]:
    style_events = [event for event in events if event["event_type"] in {"review", "tip"}]
    text_lengths = [
        len(event["text"])
        for event in style_events
        if isinstance(event["text"], str) and event["text"].strip()
    ]
    ratings = [event["rating"] for event in style_events if event["rating"] is not None]
    return {
        "avg_text_len": (sum(text_lengths) / len(text_lengths)) if text_lengths else None,
        "median_text_len": _quantile([float(value) for value in text_lengths], 0.5) if text_lengths else None,
        "rating_mean": statistics.mean(ratings) if ratings else None,
        "rating_std": statistics.pstdev(ratings) if len(ratings) > 1 else (0.0 if ratings else None),
        "positive_ratio_ge4": (sum(1 for rating in ratings if rating >= 4.0) / len(ratings)) if ratings else None,
        "text_event_count": len(text_lengths),
    }


def build_query_user_profiles(
    dataset: str,
    tables: dict[str, list[dict[str, Any]]],
    min_interactions: int,
    session_gap_hours: int,
    topk_categories: int,
) -> None:
    users = tables.get("users", [])
    interactions = tables.get("interactions", [])
    pois = tables.get("pois", [])
    poi_category_map = tables.get("poi_category_map", [])

    if not users or not interactions:
        return

    poi_latlon: dict[str, tuple[float | None, float | None]] = {}
    for poi in pois:
        poi_id = poi.get("poi_id")
        if not poi_id:
            continue
        poi_latlon[poi_id] = (
            _safe_float(poi.get("lat")),
            _safe_float(poi.get("lon")),
        )

    poi_to_categories: dict[str, list[str]] = {}
    for mapping in poi_category_map:
        poi_id = mapping.get("poi_id")
        category_id = mapping.get("category_id")
        if not poi_id or not category_id:
            continue
        poi_to_categories.setdefault(poi_id, []).append(str(category_id))

    popularity_percentiles = _popularity_percentile_map(interactions)

    interactions_by_user: dict[str, list[dict[str, Any]]] = {}
    for row in interactions:
        user_id = row.get("user_id")
        poi_id = row.get("poi_id")
        ts_raw = row.get("event_time_utc")
        if not user_id or not poi_id or not ts_raw:
            continue
        try:
            event_dt = parse_iso_utc(str(ts_raw))
        except (ValueError, TypeError):
            continue
        hour_local = row.get("hour_local")
        dow_local = row.get("dow_local")
        try:
            hour_local = int(hour_local) if hour_local is not None else event_dt.hour
        except (TypeError, ValueError):
            hour_local = event_dt.hour
        try:
            dow_local = int(dow_local) if dow_local is not None else event_dt.weekday()
        except (TypeError, ValueError):
            dow_local = event_dt.weekday()
        rating = _safe_float(row.get("rating"))
        interactions_by_user.setdefault(str(user_id), []).append(
            {
                "dt": event_dt,
                "poi_id": str(poi_id),
                "hour_local": max(0, min(23, hour_local)),
                "dow_local": max(0, min(6, dow_local)),
                "event_type": str(row.get("event_type") or "unknown"),
                "text": normalize_text(row.get("text")),
                "rating": rating,
                "category_ids": poi_to_categories.get(str(poi_id), []),
            }
        )

    for user_row in users:
        user_id = user_row.get("user_id")
        if not user_id:
            continue
        source_profile = _parse_profile_json(user_row.get("profile_json"))
        events = interactions_by_user.get(str(user_id), [])
        events.sort(key=lambda event: event["dt"])

        if len(events) < min_interactions:
            user_row["profile_json"] = None
            continue

        visited_pois: set[str] = set()
        revisited_events = 0
        first_time_visits = 0
        revisit_lags_hours: list[float] = []
        seen_time_by_poi: dict[str, datetime] = {}
        is_new_flags: list[bool] = []
        for event in events:
            poi_id = event["poi_id"]
            prev_time = seen_time_by_poi.get(poi_id)
            if prev_time is None:
                first_time_visits += 1
                is_new_flags.append(True)
            else:
                revisited_events += 1
                is_new_flags.append(False)
                revisit_lags_hours.append((event["dt"] - prev_time).total_seconds() / 3600.0)
            seen_time_by_poi[poi_id] = event["dt"]
            visited_pois.add(poi_id)

        total_events = len(events)
        recent_flags = is_new_flags[-20:]
        novelty_exploration = {
            "new_poi_rate": first_time_visits / total_events,
            "recent_new_poi_rate": (sum(1 for flag in recent_flags if flag) / len(recent_flags)) if recent_flags else None,
        }

        popularity_values = [
            popularity_percentiles[poi_id]
            for poi_id in (event["poi_id"] for event in events)
            if poi_id in popularity_percentiles
        ]
        popularity_bias = {
            "mean_popularity_percentile": (sum(popularity_values) / len(popularity_values)) if popularity_values else None,
            "top20pct_visit_fraction": (
                sum(1 for value in popularity_values if value >= 0.8) / len(popularity_values)
            ) if popularity_values else None,
        }

        sessions: list[list[int]] = []
        for idx, event in enumerate(events):
            if not sessions:
                sessions.append([idx])
                continue
            prev_idx = sessions[-1][-1]
            gap_hours = (event["dt"] - events[prev_idx]["dt"]).total_seconds() / 3600.0
            if gap_hours > session_gap_hours:
                sessions.append([idx])
            else:
                sessions[-1].append(idx)
        last_session_indices = sessions[-1]
        last_session_events = [events[idx] for idx in last_session_indices]

        last_session_category_counts: dict[str, int] = {}
        for event in last_session_events:
            for category_id in event["category_ids"]:
                last_session_category_counts[category_id] = last_session_category_counts.get(category_id, 0) + 1
        last_session_top_categories = [
            category_id
            for category_id, _ in sorted(
                last_session_category_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )[:3]
        ]
        band_counts: dict[str, int] = {"night": 0, "morning": 0, "afternoon": 0, "evening": 0}
        for event in last_session_events:
            band_counts[_time_band(event["hour_local"])] += 1
        last_session_time_band = sorted(
            band_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[0][0]
        last_session_new_flags = [is_new_flags[idx] for idx in last_session_indices]

        shared_canonical = {
            "category_distribution": _category_distribution(events, topk_categories),
            "temporal_distribution": _temporal_distribution(events),
            "spatial_radius": _spatial_radius(
                events,
                _safe_float(user_row.get("home_lat")),
                _safe_float(user_row.get("home_lon")),
                poi_latlon,
            ),
            "revisitation_ratio": revisited_events / total_events,
            "novelty_exploration": novelty_exploration,
            "popularity_bias": popularity_bias,
            "recency_preference": {
                "median_revisit_lag_hours": _quantile(revisit_lags_hours, 0.5),
                "short_lag_fraction_24h": (
                    sum(1 for lag in revisit_lags_hours if lag <= 24.0) / len(revisit_lags_hours)
                ) if revisit_lags_hours else None,
            },
            "short_term_session_intent": {
                "last_session_event_count": len(last_session_events),
                "last_session_unique_poi_count": len({event["poi_id"] for event in last_session_events}),
                "last_session_dominant_categories_top3": last_session_top_categories,
                "last_session_time_band": last_session_time_band,
                "last_session_exploration_ratio": (
                    sum(1 for flag in last_session_new_flags if flag) / len(last_session_new_flags)
                ) if last_session_new_flags else None,
            },
        }

        residuals: dict[str, Any] = {}
        if dataset == "yelp":
            residuals = {
                "yelp": {
                    "source_user_meta": source_profile,
                    "review_style": _yelp_review_style(events),
                }
            }

        profile = {
            "profile_version": "profile-v1",
            "shared_canonical": shared_canonical,
            "residuals": residuals,
            "quality": {
                "interaction_count_used": total_events,
                "profile_confidence": min(1.0, total_events / 50.0),
                "low_data_flag": total_events < 20,
            },
        }
        user_row["profile_json"] = dumps_json(profile)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def filter_tables_by_min_interactions(
    tables: dict[str, list[dict[str, Any]]],
    min_interactions: int,
) -> dict[str, int]:
    users = tables.get("users", [])
    interactions = tables.get("interactions", [])
    social_edges = tables.get("social_edges", [])
    source_mapping = tables.get("source_mapping", [])

    eligible_user_ids = {
        str(row.get("user_id"))
        for row in users
        if row.get("user_id") and _as_int(row.get("interaction_count")) >= min_interactions
    }

    filtered_users = [row for row in users if row.get("user_id") in eligible_user_ids]

    filtered_interactions = []
    for row in interactions:
        user_id = row.get("user_id")
        # Keep anonymous interactions as-is; only user-linked events are threshold-filtered.
        if user_id is None or user_id in eligible_user_ids:
            filtered_interactions.append(row)
    kept_interaction_ids = {
        str(row.get("interaction_id"))
        for row in filtered_interactions
        if row.get("interaction_id")
    }

    filtered_social_edges = [
        row
        for row in social_edges
        if row.get("src_user_id") in eligible_user_ids and row.get("dst_user_id") in eligible_user_ids
    ]
    kept_social_edge_ids = {
        str(row.get("social_edge_id"))
        for row in filtered_social_edges
        if row.get("social_edge_id")
    }

    filtered_source_mapping = []
    for row in source_mapping:
        entity_type = row.get("entity_type")
        canonical_id = row.get("canonical_id")
        if entity_type == "user":
            if canonical_id in eligible_user_ids:
                filtered_source_mapping.append(row)
            continue
        if entity_type == "interaction":
            if canonical_id in kept_interaction_ids:
                filtered_source_mapping.append(row)
            continue
        if entity_type == "social_edge":
            # Keep only social edges still present after user pruning.
            if canonical_id in kept_social_edge_ids:
                filtered_source_mapping.append(row)
            continue
        filtered_source_mapping.append(row)

    tables["users"] = filtered_users
    tables["interactions"] = filtered_interactions
    tables["social_edges"] = filtered_social_edges
    tables["source_mapping"] = filtered_source_mapping

    return {
        "users_before": len(users),
        "users_after": len(filtered_users),
        "interactions_before": len(interactions),
        "interactions_after": len(filtered_interactions),
    }
