# QuAM Eval

**QuAM Eval** stands for **Query-Aware Multi-GT Evaluation Protocol**.

This repository is building a unified POI data foundation for the QuAM
Eval protocol. The current focus is **Phase 1: raw-to-unified data
normalization**.

## Current Progress

### Done

1. Raw dataset downloader for supported defaults:
   - `foursquare_classic`
   - `gowalla`
   - `yelp`
2. Modular raw-to-unified adapter architecture:
   - `utils/data_pre_process/adapters/adapt_fsq_2014.py`
   - `utils/data_pre_process/adapters/adapt_gowalla.py`
   - `utils/data_pre_process/adapters/adapt_yelp_2024.py`
3. Unified pipeline orchestrator:
   - `utils/data_pre_process/raw2unify.py`
4. Unified phase-1 core entities:
   - `users`
   - `pois`
   - `interactions`
   - `categories`
   - `poi_category_map`
   - `social_edges`
   - `source_mapping`
5. Phase-wise docs:
   - `docs/00-Data_Download.md`
   - `docs/01_Data_Unification.md`

### In Progress / Next

1. Full `dataset/unified/v1/*` materialization run.
2. Formal unified data validators (schema/null/FK/temporal checks).
3. Query and Multi-GT construction policy (to be defined step by step).

## Quick Start

### 1) Download raw datasets

```bash
python utils/data_pre_process/download_wanted.py --datasets all
```

### 2) Build unified data (Phase 1)

```bash
python utils/data_pre_process/raw2unify.py --datasets all --out dataset/unified/v1
```

Run with file-level filtering during smoke tests:

```bash
python utils/data_pre_process/raw2unify.py --datasets gowalla --files loc-gowalla_totalCheckins.txt loc-gowalla_edges.txt --limit 50
python utils/data_pre_process/raw2unify.py --datasets yelp --files yelp_academic_dataset_review.json --limit 50
```

## Output Layout

```text
dataset/
  raw/
  unified/
    v1/
      users/
      pois/
      interactions/
      categories/
      poi_category_map/
      social_edges/
      source_mapping/
      metadata/
```

## Documentation

1. Download phase: `docs/00-Data_Download.md`
2. Unification phase: `docs/01_Data_Unification.md`
3. Execution plan: `.agent/PLANS.md`
