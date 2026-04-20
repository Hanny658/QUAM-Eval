## Phase 0 - Data Download

This phase only fetches raw datasets into `dataset/raw/*`.
No schema normalization is done here.

### Supported datasets

1. `foursquare_classic`
2. `gowalla`
3. `yelp`

### Command examples

```bash
python utils/data_pre_process/download_wanted.py --datasets all
python utils/data_pre_process/download_wanted.py --datasets foursquare_classic gowalla
python utils/data_pre_process/download_wanted.py --datasets yelp
```

### Expected raw layout

```text
dataset/raw/
  foursquare_classic/
  gowalla/
  yelp/
```

### Notes

1. `yelp` download uses Kaggle credentials.
2. Raw files are kept source-native and are not modified.
3. Unification is handled in phase-1 by `raw2unify.py`.
