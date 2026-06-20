# superset_config.py

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://airflow:airflow@postgres:5432/superset"
)

SECRET_KEY = "weather-forecast-superset-secret"
