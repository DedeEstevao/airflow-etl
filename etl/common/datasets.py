from airflow import Dataset

# =====================================================
# RAW
# =====================================================

raw_forecast_dataset = Dataset(
    "postgres://postgres/airflow/raw/weather_forecast"
)

raw_observed_dataset = Dataset(
    "postgres://postgres/airflow/raw/weather_observed"
)

# =====================================================
# STAGING
# =====================================================

staging_forecast_dataset = Dataset(
    "postgres://postgres/airflow/staging/weather_forecast"
)

staging_observed_dataset = Dataset(
    "postgres://postgres/airflow/staging/weather_observed"
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
