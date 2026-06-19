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
        FROM staging.weather_observed;
    """

    watermark = hook.get_first(sql)[0]

    logger.info(f"Watermark retrieved: {watermark}")
    return watermark


def insert_incremental_observed(
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
            INSERT INTO staging.weather_observed (
              city,
              latitude,
              longitude,
              observation_datetime,
              observed_temperature,
              observed_rain,
              raw_payload_hash,
              raw_ingested_at
            )
            SELECT
              r.city,
              r.latitude,
              r.longitude,

              (t.time_value)::timestamptz AS observation_datetime,

              weather.temperature,
              weather.rain,

              r.payload_hash,

              date_trunc('hour', r.ingested_at) 
                as raw_ingested_at

            FROM raw.weather_observed r

            CROSS JOIN LATERAL
             jsonb_array_elements_text(r.payload->'hourly'->'time')
             WITH ORDINALITY AS t(time_value, idx)

            LEFT JOIN LATERAL (
            SELECT
                (
                 r.payload->'hourly'
                 ->'temperature_2m'
                   ->>((idx - 1)::int)
                )::numeric AS temperature,

                (
                 r.payload->'hourly'
                 ->'rain'
                 ->>((idx - 1)::int)
                )::numeric AS rain

            ) AS weather ON TRUE

            WHERE r.ingested_at > %s
        
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


def load_observed_to_staging(postgres_conn_id: str = "open_meteo"):
    """
    Main entrypoint for Airflow task.
    """

    logger.info("Starting staging load...")

    watermark = get_last_processed_timestamp(postgres_conn_id)
    rows_inserted = insert_incremental_observed(
        postgres_conn_id,
        watermark
    )

    logger.info(
        f"Staging load finished successfully | "
        f"Watermark used: {watermark} | "
        f"Rows inserted: {rows_inserted}"
    )

    return rows_inserted
