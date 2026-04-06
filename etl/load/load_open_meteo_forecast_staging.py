from datetime import datetime, timezone
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_last_processed_timestamp(postgres_conn_id: str) -> datetime:
    """
    Retrieves the latest raw_ingested_at processed in staging.
    """

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    sql = """
        SELECT COALESCE(
            MAX(raw_ingested_at),
            TIMESTAMPTZ '1900-01-01 00:00:00+00'
        )
        FROM staging.open_meteo_forecast;
    """

    watermark = hook.get_first(sql)[0]

    logger.info(f"Watermark retrieved: {watermark}")
    return watermark


def insert_incremental_forecast(
    postgres_conn_id: str,
    watermark: datetime,
) -> int:
    """
    Inserts new RAW records into STAGING based on watermark.
    Returns the exact number of inserted rows.
    """

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    sql = """
        WITH inserted AS (
            INSERT INTO staging.open_meteo_forecast (
              city,
              latitude,
              longitude,
              model_run_datetime,
              forecast_datetime,
              temperature,
              precipitation,
              raw_payload_hash,
              raw_ingested_at
           )
           SELECT
             r.city,
             r.latitude,
             r.longitude,
             date_trunc('day', r.ingested_at)
                 + ((extract(hour from r.ingested_at)::int / 6) * interval '6 hours')
                 AS model_run_datetime,
             (t.time_value)::timestamptz AS forecast_datetime,
             weather.temperature,
             weather.precipitation,
             r.payload_hash,
             date_trunc('hour', r.ingested_at)
           FROM raw.open_meteo_forecast r

           CROSS JOIN LATERAL
             jsonb_array_elements_text(r.payload::jsonb->'hourly'->'time')
             WITH ORDINALITY AS t(time_value, idx)

           LEFT JOIN LATERAL (
             SELECT
               (r.payload->'hourly'->'temperature_2m'->>((idx-1)::int))::numeric AS temperature,
               (r.payload->'hourly'->'precipitation_probability'->>((idx-1)::int))::numeric AS precipitation
           ) AS weather ON TRUE

           WHERE r.ingested_at >= %s
        
           ON CONFLICT DO NOTHING
           
           RETURNING 1
          
        )
        SELECT count(*) FROM inserted;
    """

    start_time = datetime.now(timezone.utc)

    try:
        cursor.execute(sql, (watermark,))
        rows_inserted = cursor.fetchone()[0]
        conn.commit()

    except Exception:
        conn.rollback()
        logger.exception("Error during staging insert")
        raise

    finally:
        cursor.close()
        conn.close()

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    logger.info(
        f"Staging insert completed | "
        f"Rows inserted: {rows_inserted} | "
        f"Watermark: {watermark} | "
        f"Duration: {duration:.2f}s"
    )

    return rows_inserted


def load_staging(postgres_conn_id: str = "postgres_default"):
    """
    Main entrypoint for Airflow task.
    """

    logger.info("Starting staging load...")

    watermark = get_last_processed_timestamp(postgres_conn_id)
    rows_inserted = insert_incremental_forecast(
        postgres_conn_id,
        watermark
    )

    logger.info(
        f"Staging load finished successfully | "
        f"Watermark used: {watermark} | "
        f"Rows inserted: {rows_inserted}"
    )

    return rows_inserted

