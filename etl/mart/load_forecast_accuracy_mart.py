from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_forecast_accuracy_mart(postgres_conn_id: str = "open_meteo"):

    logger.info("Starting MART load...")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    sql = """
    WITH joined AS (
     SELECT
        f.city,
        f.latitude,
        f.longitude,

        f.forecast_datetime,
        f.model_run_datetime,

        f.temperature AS predicted_temperature,
        f.rain AS predicted_rain,
        f.precipitation_probability,

        o.observed_temperature,
        o.observed_rain,

        f.raw_ingested_at AS raw_forecast_ingested_at,
        o.raw_ingested_at AS raw_observed_ingested_at

     FROM staging.weather_forecast f
   
     INNER JOIN staging.weather_observed o
        ON f.latitude = o.latitude
       AND f.longitude = o.longitude
       AND f.forecast_datetime = o.observation_datetime

     WHERE
         f.forecast_datetime >= f.model_run_datetime
         AND f.temperature IS NOT NULL
         AND o.observed_temperature IS NOT NULL
         AND f.rain IS NOT NULL
         AND o.observed_rain IS NOT NULL
    ),


   inserted AS (

      INSERT INTO mart.weather_forecast_accuracy (
         city,
         latitude,
         longitude,
         forecast_datetime,
         model_run_datetime,
         predicted_temperature,
         predicted_rain,
         precipitation_probability,
         observed_temperature,
         observed_rain,
         raw_forecast_ingested_at,
         raw_observed_ingested_at
      )

      SELECT
         city,
         latitude,
         longitude,
         forecast_datetime,
         model_run_datetime,
         predicted_temperature,
         predicted_rain,
         precipitation_probability,
         observed_temperature,
         observed_rain,
         raw_forecast_ingested_at,
         raw_observed_ingested_at
      FROM joined

      ON CONFLICT DO NOTHING

      RETURNING 1
   )
   
   SELECT COUNT(*)
   FROM inserted;
"""
    

    cursor.execute(sql)
    rows_inserted = cursor.fetchone()[0]

    conn.commit()

    logger.info(f"MART load completed. Rows inserted/updated: {rows_inserted}")

    cursor.close()
    conn.close()

    return rows_inserted
