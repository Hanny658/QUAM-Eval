# Dataset Description

## Dataset Overview

This folder shall contain different POI datasets like Foursquare / Gowalla / Yelp etc.
Each dataset should be stored in `dataset/raw` in a separate subfolder named after the dataset (e.g., `foursquare`, `gowalla`, `yelp`).
The dowload script is provided in `utils\data_pre_process\download_wanted.py` which can be used to download the datasets from their respective sources to this folder.

## Dataset Supported

- [Foursquare](https://sites.google.com/site/yangdingqi/home/foursquare-dataset): Popular POI dataset that its subsets are often refered as NYC, TKY, and CA (TSMC2014).
- [Gowalla](https://snap.stanford.edu/data/loc-gowalla.html): Another popular POI dataset that contains check-in data from the Gowalla location-based social networking website.
- [Yelp](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset/code): A newer dataset that contains business information, reviews, and user data from Yelp.

> for newer datasets, it is technically supported by QUAM-Eval as long as the dataset contains the necessary information such as POI names, locations, and categories. However, the evaluation results may vary depending on the quality and structure of the dataset, so it is recommended to use well-known and widely used datasets for more reliable evaluation results. To accomadate newer datasets, please write your own adapter to convert the dataset into the required format for QuAM-Eval.
