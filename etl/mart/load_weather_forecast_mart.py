from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_mart(postgres_conn_id: str = "open_meteo"):

    logger.info("Starting MART load...")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Watermark
    cursor.execute("""
        SELECT COALESCE(
            MAX(model_run_datetime),
            TIMESTAMPTZ '1900-01-01 00:00:00+00'
        )
        FROM mart.weather_forecast;
    """)

    watermark = cursor.fetchone()[0]
    logger.info(f"Watermark MART: {watermark}")

    # Insert incremental
    sql = """
    WITH filtered AS (
        SELECT *
        FROM staging.weather_forecast s
        WHERE
            s.model_run_datetime >= %s
            AND s.temperature IS NOT NULL
            AND s.rain IS NOT NULL
            AND COALESCE(s.precipitation_probability, 0) BETWEEN 0 AND 100
    ),

    deduplicated AS (
        SELECT DISTINCT ON (latitude, longitude, forecast_datetime)
            city,
            latitude,
            longitude,
            forecast_datetime,
            temperature,
            precipitation_probability,
            rain,
            model_run_datetime
        FROM filtered
        ORDER BY
            latitude,
            longitude,
            forecast_datetime,
            model_run_datetime DESC
    ),

    inserted AS (
        INSERT INTO mart.weather_forecast (
            city,
            latitude,
            longitude,
            forecast_datetime,
            temperature,
            rain,
            precipitation_probability,
            model_run_datetime
        )

        SELECT
            city,
            latitude,
            longitude,
            forecast_datetime,
            temperature,
            rain,
            precipitation_probability,
            model_run_datetime
        FROM deduplicated

        ON CONFLICT (latitude, longitude, forecast_datetime)
        DO UPDATE SET
            temperature = EXCLUDED.temperature,
            precipitation_probability = EXCLUDED.precipitation_probability,
            rain = EXCLUDED.rain,
            model_run_datetime = EXCLUDED.model_run_datetime

        RETURNING 1
    )

    SELECT COUNT(*) FROM inserted;
    """

    try:
        cursor.execute(sql, (watermark,))
        rows_inserted = cursor.fetchone()[0]

        conn.commit()

    except Exception:
        conn.rollback()
        logger.exception("Error loading MART")
        raise

    finally:
        cursor.close()
        conn.close()

    return rows_inserted
