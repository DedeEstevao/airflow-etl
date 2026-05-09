from airflow import Dataset

# =====================================================
# RAW
# =====================================================

raw_dataset = Dataset(
    "postgres://postgres/airflow/raw/open_meteo_forecast"
)

# =====================================================
# STAGING
# =====================================================

staging_dataset = Dataset(
    "postgres://postgres/airflow/staging/open_meteo_forecast"
)

# =====================================================
# MART
# =====================================================

mart_dataset = Dataset(
    "postgres://postgres/airflow/mart/open_meteo_forecast"
)

# =====================================================
# ANALYTICS
# =====================================================

weather_daily_dataset = Dataset(
    "postgres://postgres/airflow/analytics/weather_daily"
)

weather_features_dataset = Dataset(
    "postgres://postgres/airflow/analytics/weather_features"
)

# =====================================================
# MACHINE LEARNING
# =====================================================

model_metadata_dataset = Dataset(
    "postgres://postgres/airflow/analytics/model_metadata"
)