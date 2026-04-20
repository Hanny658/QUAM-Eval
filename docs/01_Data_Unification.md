## Phase 1 - Raw to Unified

Phase-1 builds a unified POI data foundation from `dataset/raw/*` to
`dataset/unified/v1/*`.

### Unified entities

1. `users`
2. `pois`
3. `interactions`
4. `categories`
5. `poi_category_map`
6. `social_edges`
7. `source_mapping`

`social_edges` keeps user graph relations for datasets that provide
social network information.

### Entity field schema

#### `users`

| field | type | required | description |
| --- | --- | --- | --- |
| `user_id` | string | yes | Canonical user ID (stable hash/UUID-style ID). |
| `dataset` | string | yes | Source dataset name, e.g. `foursquare_classic`, `gowalla`, `yelp`. |
| `source_user_id` | string | yes | Original user identifier from raw data. |
| `first_event_time_utc` | timestamp (ISO8601 string) | no | Earliest known interaction time in UTC. |
| `last_event_time_utc` | timestamp (ISO8601 string) | no | Latest known interaction time in UTC. |
| `interaction_count` | int64 | no | Number of interactions attributed to this user. |
| `active_days` | int32 | no | Distinct active local dates. |
| `home_lat` | float64 | no | Mean latitude inferred from user interactions. |
| `home_lon` | float64 | no | Mean longitude inferred from user interactions. |
| `profile_json` | string (JSON) | no | Dataset-specific profile payload (stats/metadata). |

#### `pois`

| field | type | required | description |
| --- | --- | --- | --- |
| `poi_id` | string | yes | Canonical POI ID. |
| `dataset` | string | yes | Source dataset name. |
| `source_poi_id` | string | yes | Original POI/business/location ID from raw data. |
| `name` | string | no | POI/business name if available. |
| `lat` | float64 | no | WGS84 latitude. |
| `lon` | float64 | no | WGS84 longitude. |
| `geohash7` | string | no | Geohash precision 7 for spatial indexing. |
| `city` | string | no | City field if available. |
| `state` | string | no | State/province field if available. |
| `country` | string | no | Country field if available. |
| `timezone` | string | no | IANA timezone if available. |
| `is_active` | bool | no | Open/active indicator when source provides it. |
| `poi_meta_json` | string (JSON) | no | Extended POI metadata (hours, attributes, split tag, etc.). |

#### `interactions`

| field | type | required | description |
| --- | --- | --- | --- |
| `interaction_id` | string | yes | Canonical interaction/event ID. |
| `dataset` | string | yes | Source dataset name. |
| `source_interaction_id` | string | no | Source event identifier (review/tip/checkin ID). |
| `user_id` | string | conditional | Canonical user ID; may be null for anonymous aggregate events (e.g. Yelp checkin rows). |
| `poi_id` | string | yes | Canonical POI ID. |
| `event_time_utc` | timestamp (ISO8601 string) | yes | Event time normalized to UTC. |
| `event_time_local` | timestamp (ISO8601 string) | no | Local event timestamp if derivable. |
| `timezone_offset_min` | int32 | no | Timezone offset minutes when available. |
| `local_date` | date (YYYY-MM-DD) | no | Local calendar date derived from event time. |
| `dow_local` | int8 | no | Day of week in local time (`0-6`). |
| `hour_local` | int8 | no | Hour in local time (`0-23`). |
| `event_type` | string | yes | Event type (`checkin`, `review`, `tip`, etc.). |
| `text` | string | no | Interaction text (review/tip body). |
| `rating` | float32 | no | Numeric rating when provided by source. |
| `geo_distance_prev_km` | float32 | no | Reserved derived feature; currently null in adapters. |
| `delta_time_prev_min` | float32 | no | Reserved derived feature; currently null in adapters. |
| `interaction_meta_json` | string (JSON) | no | Extra event metadata payload. |

#### `categories`

| field | type | required | description |
| --- | --- | --- | --- |
| `category_id` | string | yes | Canonical category ID. |
| `dataset` | string | yes | Source dataset name. |
| `source_category_id` | string | no | Original category ID from source, if available. |
| `name` | string | yes | Category display name. |
| `parent_category_id` | string | no | Parent category ID for hierarchical taxonomy. |
| `taxonomy_level` | int16 | no | Optional hierarchy level/depth. |
| `canonical_group` | string | no | Optional harmonized super-category label. |

#### `poi_category_map`

| field | type | required | description |
| --- | --- | --- | --- |
| `dataset` | string | yes | Source dataset name. |
| `poi_id` | string | yes | Canonical POI ID. |
| `category_id` | string | yes | Canonical category ID. |
| `is_primary` | bool | no | Whether the category is primary for the POI. |
| `confidence` | float32 | no | Mapping confidence score. |

#### `social_edges`

| field | type | required | description |
| --- | --- | --- | --- |
| `social_edge_id` | string | yes | Canonical social edge ID. |
| `dataset` | string | yes | Source dataset name. |
| `source_edge_id` | string | no | Raw edge ID/signature (if available). |
| `src_user_id` | string | yes | Canonical source endpoint user ID. |
| `dst_user_id` | string | yes | Canonical destination endpoint user ID. |
| `relation_type` | string | yes | Relation label, e.g. `friend`. |
| `is_directed` | bool | yes | Whether edge is directed. |
| `created_time_utc` | timestamp (ISO8601 string) | no | Edge creation timestamp if available. |
| `edge_weight` | float32 | no | Optional edge weight/strength. |
| `edge_meta_json` | string (JSON) | no | Extra relation metadata. |

#### `source_mapping`

| field | type | required | description |
| --- | --- | --- | --- |
| `dataset` | string | yes | Source dataset name. |
| `entity_type` | string | yes | Entity type (`user`, `poi`, `category`, `interaction`, `social_edge`). |
| `source_id` | string | yes | Raw/source ID for the entity. |
| `canonical_id` | string | yes | Canonical unified ID mapped from `source_id`. |

### Adapter architecture

Dataset-specific parsing is implemented in:

1. `utils/data_pre_process/adapters/adapt_fsq_2014.py`
2. `utils/data_pre_process/adapters/adapt_gowalla.py`
3. `utils/data_pre_process/adapters/adapt_yelp_2024.py`

`utils/data_pre_process/raw2unify.py` is the orchestrator:

1. resolve dataset/file args
2. invoke adapters
3. write unified outputs
4. generate metadata manifest

### CLI

```bash
python utils/data_pre_process/raw2unify.py --datasets all
python utils/data_pre_process/raw2unify.py --datasets gowalla --files loc-gowalla_totalCheckins.txt loc-gowalla_edges.txt
python utils/data_pre_process/raw2unify.py --datasets yelp --files "yelp_academic_dataset_business.json" "yelp_academic_dataset_user.json"
```

Key args:

1. `--datasets`: select datasets (`all` or explicit names)
2. `--files`: select specific files to process (path/glob/basename)
3. `--out`: unified output root
4. `--format`: `jsonl` or `parquet`
5. `--limit`: smoke-test cap for interaction events (dimension tables
   may still scan selected source files)
6. `--strict` / `--strict-files`: strict input checks
7. `--force`: overwrite existing output partitions

### Output layout

```text
dataset/unified/v1/
  users/
  pois/
  interactions/
  categories/
  poi_category_map/
  social_edges/
  source_mapping/
  metadata/
    schema_version.json
    build_manifest.json
```

### Phase boundary

This phase does not implement Query construction or Multi-GT generation.
Those are deferred to phase-2 with user-defined policy.
