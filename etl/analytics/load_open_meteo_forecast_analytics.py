from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)


def load_analytics(postgres_conn_id="postgres_default"):

    logger.info("Starting ANALYTICS load...")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    sql = """
    WITH aggregated AS (
        SELECT
            city,
            DATE(forecast_datetime) AS date,
            AVG(temperature) AS avg_temperature,
            MIN(temperature) AS min_temperature,
            MAX(temperature) AS max_temperature,
            AVG(precipitation_probability) AS avg_precipitation_probability,
            MAX(model_run_datetime) AS model_run_datetime
        FROM mart.open_meteo_forecast
        GROUP BY city, DATE(forecast_datetime)
    ),

    inserted AS (
        INSERT INTO analytics.weather_daily (
            city,
            date,
            avg_temperature,
            min_temperature,
            max_temperature,
            avg_precipitation_probability,
            model_run_datetime
        )
        SELECT *
        FROM aggregated

        ON CONFLICT (city, date)
        DO UPDATE SET
            avg_temperature = EXCLUDED.avg_temperature,
            min_temperature = EXCLUDED.min_temperature,
            max_temperature = EXCLUDED.max_temperature,
            avg_precipitation_probability = EXCLUDED.avg_precipitation_probability,
            model_run_datetime = EXCLUDED.model_run_datetime

        RETURNING 1
    )

    SELECT COUNT(*) FROM inserted;
    """

    cursor.execute(sql)
    rows = cursor.fetchone()[0]
    conn.commit()

    logger.info(f"ANALYTICS load completed. Rows: {rows}")

    cursor.close()
    conn.close()

    return rows