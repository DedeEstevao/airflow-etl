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

    try:

        # Filtra apenas registros da execução atual
        base_filter = """
         created_at >= %s
         AND created_at < %s
        """

        # 1️⃣ Null check
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM staging.weather_observed
            WHERE observation_datetime IS NULL
            AND {base_filter}
        """, (interval_start, interval_end))

        if cursor.fetchone()[0] > 0:
            raise ValueError("DQ Failed: NULL observation_datetime")

        # 2️⃣ Temperature range
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM staging.weather_observed
            WHERE (observed_temperature < -60 OR observed_temperature > 60)
            AND {base_filter}
        """, (interval_start, interval_end))

        if cursor.fetchone()[0] > 0:
            raise ValueError("DQ Failed: Temperature out of range")

        # 3️⃣ Negative observed_rain
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM staging.weather_observed
            WHERE observed_rain < 0
            AND {base_filter}
        """, (interval_start, interval_end))

        if cursor.fetchone()[0] > 0:
            raise ValueError("DQ Failed: Negative rain observation")

        # 4️⃣ Deve haver registros novos
        cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM staging.weather_observed
            WHERE observation_datetime IS NULL
            AND created_at >= %s
            AND created_at < %s
        )
        """, (interval_start, interval_end))

        rowcount = cursor.fetchone()[0]
        if rowcount == 0:
                logger.warning(
                    "DQ Warning: No new records in this run"
                )
                return

        logger.info(
            f"DQ Passed | Rows checked: {rowcount}"
        )

    finally:

        cursor.close()
        conn.close()
