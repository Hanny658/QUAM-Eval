## Phase 1.5 - User Profile Buildup

This document defines how `users.profile_json` is constructed in phase-1
for query-oriented downstream tasks.

## Build location

Profile buildup is executed inside `raw2unify.py` through the shared
common-layer function:

1. `utils/data_pre_process/adapters/common.py::build_query_user_profiles`
2. Called after dataset adapter output and before unified table write.

## Global constants

Configured at the top of `utils/data_pre_process/raw2unify.py`:

1. `PROFILE_MIN_INTERACTIONS = 10`
2. `PROFILE_SESSION_GAP_HOURS = 6`
3. `PROFILE_TOPK_CATEGORIES = 20`

Rule:

1. Users with `interaction_count < 10` are filtered out at build time:
   they are not written into `users`, and their user-linked interactions
   are not written into `interactions`.
2. Therefore, users remaining in `users` all satisfy the minimum
   interaction threshold.

## `profile_json` schema

```json
{
  "profile_version": "profile-v1",
  "shared_canonical": {
    "category_distribution": {},
    "temporal_distribution": {},
    "spatial_radius": {},
    "revisitation_ratio": 0.0,
    "novelty_exploration": {},
    "popularity_bias": {},
    "recency_preference": {},
    "short_term_session_intent": {}
  },
  "residuals": {},
  "quality": {
    "interaction_count_used": 0,
    "profile_confidence": 0.0,
    "low_data_flag": false
  }
}
```

## Shared canonical metrics

Input event set per user:

1. `interactions` rows where both `user_id` and `poi_id` are non-null.
2. Sorted by `event_time_utc`.
3. Since low-activity users are filtered before profile buildup, this
   event set is always at least 10 user-linked interactions.

### 1) `category_distribution`

1. POI categories come from `poi_category_map`.
2. If no category mapping exists, use empty payload.
3. Output:
   - `topk_probs`: top `PROFILE_TOPK_CATEGORIES` probabilities
   - `entropy`: `-sum(p * ln(p))`
   - `observed_category_events`

### 2) `temporal_distribution`

1. `hour_hist_24`: normalized histogram over local hour.
2. `dow_hist_7`: normalized histogram over local day-of-week.
3. `weekend_ratio`: `(events on dow 5/6) / total_events`.

### 3) `spatial_radius`

1. Home center from `users.home_lat/home_lon`.
2. Distances are Haversine(home, POI) in km.
3. Output:
   - `radius_of_gyration_km = sqrt(mean(distance_km^2))`
   - `p50_km`
   - `p90_km`
4. If home or POI coordinates are missing, these fields are `null`.

### 4) `revisitation_ratio`

1. Event is revisited if its POI appeared previously for the same user.
2. Formula: `revisited_events / total_events`.

### 5) `novelty_exploration`

1. `new_poi_rate = first_time_visits / total_events`.
2. `recent_new_poi_rate`: same ratio over latest 20 events (or fewer).

### 6) `popularity_bias`

1. Dataset-global POI popularity is defined by total interaction
   frequency (current run scope).
2. Convert popularity to percentile rank in `[0,1]`.
3. Output:
   - `mean_popularity_percentile`
   - `top20pct_visit_fraction` (fraction of events with percentile >= 0.8)

### 7) `recency_preference`

1. For each revisit, lag is measured from previous same-POI timestamp.
2. Output:
   - `median_revisit_lag_hours`
   - `short_lag_fraction_24h`

### 8) `short_term_session_intent`

1. Session split: open a new session when gap > 6 hours.
2. Use only the latest session.
3. Output:
   - `last_session_event_count`
   - `last_session_unique_poi_count`
   - `last_session_dominant_categories_top3`
   - `last_session_time_band` (`night|morning|afternoon|evening`)
   - `last_session_exploration_ratio`

## Residuals contract

1. Residuals must be under `residuals.<dataset_name>`.
2. Non-Yelp datasets currently use empty object `{}`.
3. Yelp residuals include:
   - `residuals.yelp.source_user_meta` (original user-side metadata)
   - `residuals.yelp.review_style` with:
     - `avg_text_len`
     - `median_text_len`
     - `rating_mean`
     - `rating_std`
     - `positive_ratio_ge4`
     - `text_event_count`

## Quality contract

1. `interaction_count_used`: profiled event count.
2. `profile_confidence = min(1.0, interaction_count_used / 50.0)`.
3. `low_data_flag = interaction_count_used < 20`.
