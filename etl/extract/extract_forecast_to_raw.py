import hashlib
import json
import logging
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def extract_forecast_to_raw(latitude: float, longitude: float) -> dict:
    logger.info("Extraindo dados da API Open-Meteo")

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,precipitation_probability,rain",
        "models": "gfs_seamless",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def generate_payload_hash(payload: dict) -> str:

    payload_str = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(payload_str.encode()).hexdigest()


def load_forecast_weather_raw(
    city: str,
    latitude: float,
    longitude: float,
    payload: dict,
    dag_run_id: str,
    postgres_conn_id: str = "open_meteo",


):
    logger.info("Carregando dados Forecast no Postgres")
    payload_hash = generate_payload_hash(payload)

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    insert_sql = """
            INSERT INTO raw.weather_forecast
            (city, latitude, longitude, payload, payload_hash,dag_run_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (latitude, longitude, payload_hash) DO NOTHING;

        """

    pg_hook.run(
        insert_sql,
        parameters=(
            city,
            latitude,
            longitude,
            json.dumps(payload),
            payload_hash,
            dag_run_id
        ),
    )

    logger.info("Carga Open-Meteo finalizada com sucesso")
