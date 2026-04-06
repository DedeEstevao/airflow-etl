from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)


def check_staging_quality_incremental(
    postgres_conn_id: str,
    interval_start,
    interval_end,
):

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Filtra apenas registros da execução atual
    base_filter = """
        created_at >= %s
        AND created_at < %s
    """

    # 1️⃣ Null check
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM staging.open_meteo_forecast
        WHERE forecast_datetime IS NULL
        AND {base_filter}
    """, (interval_start, interval_end))

    if cursor.fetchone()[0] > 0:
        raise ValueError("DQ Failed: NULL forecast_datetime")

    # 2️⃣ Temperature range
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM staging.open_meteo_forecast
        WHERE (temperature < -60 OR temperature > 60)
        AND {base_filter}
    """, (interval_start, interval_end))

    if cursor.fetchone()[0] > 0:
        raise ValueError("DQ Failed: Temperature out of range")

    # 3️⃣ Negative precipitation
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM staging.open_meteo_forecast
        WHERE precipitation < 0
        AND {base_filter}
    """, (interval_start, interval_end))

    if cursor.fetchone()[0] > 0:
        raise ValueError("DQ Failed: Negative precipitation")

    # 4️⃣ Deve haver registros novos
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM staging.open_meteo_forecast
        WHERE {base_filter}
    """, (interval_start, interval_end))

    rowcount = cursor.fetchone()[0]

    if rowcount == 0:
        logger.warning("DQ Warning: No new records in this run")
        return

    cursor.close()
    conn.close()