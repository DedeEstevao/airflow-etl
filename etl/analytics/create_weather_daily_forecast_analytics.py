from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)


def load_daily_analytics(postgres_conn_id="open_meteo"):

    logger.info("Starting ANALYTICS load...")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    sql = """
    WITH aggregated AS (
        SELECT
            city,
            latitude,
            longitude,
            DATE(forecast_datetime) AS date,
            AVG(temperature) AS avg_temperature,
            MIN(temperature) AS min_temperature,
            MAX(temperature) AS max_temperature,
            AVG(precipitation_probability) AS avg_precipitation_probability,
            SUM(rain) AS total_rain,
            MAX(rain) AS max_rain,
            MAX(model_run_datetime) AS model_run_datetime
        FROM mart.weather_forecast
        GROUP BY city, latitude, longitude, DATE(forecast_datetime)
    ),

    inserted AS (
        INSERT INTO analytics.weather_daily (
            city,
            latitude,
            longitude,
            forecast_date,
            avg_temperature,
            min_temperature,
            max_temperature,
            avg_precipitation_probability,
            total_rain,
            max_rain,
            model_run_datetime
        )
        
        SELECT
            city,
            latitude,
            longitude,
            date,
            avg_temperature,
            min_temperature,
            max_temperature,
            avg_precipitation_probability,
            total_rain,
            max_rain,
            model_run_datetime
        FROM aggregated

        ON CONFLICT (latitude, longitude, forecast_date)
        DO UPDATE SET
            avg_temperature = EXCLUDED.avg_temperature,
            min_temperature = EXCLUDED.min_temperature,
            max_temperature = EXCLUDED.max_temperature,
            avg_precipitation_probability = EXCLUDED.avg_precipitation_probability,
            total_rain = EXCLUDED.total_rain,
            max_rain = EXCLUDED.max_rain,
            model_run_datetime = EXCLUDED.model_run_datetime

        RETURNING 1
    )

    SELECT COUNT(*) FROM inserted;
    """

    try:
        cursor.execute(sql,)
        rows_inserted = cursor.fetchone()[0]

        conn.commit()

    except Exception:
        conn.rollback()
        logger.exception("Error loading ANALYTICS weather daily data")
        raise

    finally:
        cursor.close()
        conn.close()

    return rows_inserted
