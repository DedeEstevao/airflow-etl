import hashlib
import json
import logging
import requests
from datetime import datetime, timedelta, timezone
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def extract_observed_weather(
    latitude: float,
    longitude: float,
    start_date: datetime.date,
    end_date: datetime.date ,
) -> dict:
    logger.info("Extraindo dados da API Open-Meteo Observed")

    url = (
        "https://archive-api.open-meteo.com/"
        "v1/archive"
    )

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": "temperature_2m,rain",
        "timezone": "auto"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def generate_payload_hash(payload: dict) -> str:
    
    payload_str = json.dumps(
        payload["hourly"],
        sort_keys=True
    )

    return hashlib.sha256(payload_str.encode()).hexdigest()


def load_observed_weather(
    city: str,
    latitude: float,
    longitude: float,
    payload: dict,
    dag_run_id: str,
    postgres_conn_id: str = "open_meteo",


):
    logger.info("Carregando dados Observed no Postgres")
    payload_hash = generate_payload_hash(payload)

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    insert_sql = """
            INSERT INTO raw.weather_observed
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
