from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_mart(postgres_conn_id: str = "postgres_default"):

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
        FROM mart.open_meteo_forecast;
    """)

    watermark = cursor.fetchone()[0]
    logger.info(f"Watermark MART: {watermark}")

    # Insert incremental
    sql = """
    WITH filtered AS (
        SELECT *
        FROM staging.open_meteo_forecast s
        WHERE
            s.model_run_datetime >= %s
            AND s.temperature IS NOT NULL
            AND COALESCE(s.precipitation, 0) BETWEEN 0 AND 100
    ),

    deduplicated AS (
        SELECT DISTINCT ON (latitude, longitude, forecast_datetime)
            city,
            latitude,
            longitude,
            forecast_datetime,
            temperature,
            precipitation,
            model_run_datetime
        FROM filtered
        ORDER BY
            latitude,
            longitude,
            forecast_datetime,
            model_run_datetime DESC
    ),

    inserted AS (
        INSERT INTO mart.open_meteo_forecast (
            city,
            latitude,
            longitude,
            forecast_datetime,
            temperature,
            precipitation_probability,
            model_run_datetime
        )
        SELECT *
        FROM deduplicated

        ON CONFLICT (latitude, longitude, forecast_datetime)
        DO UPDATE SET
            temperature = EXCLUDED.temperature,
            precipitation_probability = EXCLUDED.precipitation_probability,
            model_run_datetime = EXCLUDED.model_run_datetime

        RETURNING 1
    )

    SELECT COUNT(*) FROM inserted;
    """

    cursor.execute(sql, (watermark,))
    rows_inserted = cursor.fetchone()[0]

    conn.commit()

    logger.info(f"MART load completed. Rows inserted/updated: {rows_inserted}")

    cursor.close()
    conn.close()

    return rows_inserted
