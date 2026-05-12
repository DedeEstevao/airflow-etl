from airflow.providers.postgres.hooks.postgres import (
    PostgresHook,
)

import logging

logger = logging.getLogger(__name__)


def generate_weather_features(
    postgres_conn_id="postgres_default",
):

    logger.info(
        "Starting ANALYTICS weather features load..."
    )

    hook = PostgresHook(
        postgres_conn_id=postgres_conn_id
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    truncate_sql = """
    TRUNCATE TABLE analytics.weather_features;
    """

    insert_sql = """
    WITH latest_forecast AS (

        SELECT *
        FROM (

            SELECT
                city,
                latitude,
                longitude,
                forecast_datetime,
                temperature,
                precipitation_probability
                    AS precipitation,
                model_run_datetime,

                ROW_NUMBER() OVER (
                    PARTITION BY
                        city,
                        forecast_datetime
                    ORDER BY
                        model_run_datetime DESC
                ) AS rn

            FROM mart.open_meteo_forecast

        ) ranked

        WHERE rn = 1
    ),

    feature_base AS (

        SELECT
            city,

            latitude,

            longitude,

            forecast_datetime,

            temperature,

            precipitation,

            -- =========================================
            -- TEMPORAL FEATURES
            -- =========================================

            EXTRACT(
                HOUR
                FROM forecast_datetime
            ) AS hour,

            SIN(
                2 * PI()
                * EXTRACT(
                    HOUR
                    FROM forecast_datetime
                ) / 24
            ) AS hour_sin,

            COS(
                2 * PI()
                * EXTRACT(
                    HOUR
                    FROM forecast_datetime
                ) / 24
            ) AS hour_cos,

            -- =========================================
            -- TEMPERATURE LAGS
            -- =========================================

            LAG(
                temperature, 1
            )
            OVER weather_window
            AS temp_lag_1,

            LAG(
                temperature, 2
            )
            OVER weather_window
            AS temp_lag_2,

            LAG(
                temperature, 3
            )
            OVER weather_window
            AS temp_lag_3,

            LAG(
                temperature, 6
            )
            OVER weather_window
            AS temp_lag_6,

            -- =========================================
            -- PRECIPITATION LAGS
            -- =========================================

            LAG(
                precipitation, 1
            )
            OVER weather_window
            AS precip_lag_1,

            LAG(
                precipitation, 2
            )
            OVER weather_window
            AS precip_lag_2,

            -- =========================================
            -- DIFFERENCE
            -- =========================================

            temperature
            -
            LAG(
                temperature, 1
            )
            OVER weather_window
            AS temp_diff,

            -- =========================================
            -- ROLLING MEANS
            -- =========================================

            AVG(temperature)
            OVER (
                weather_window
                ROWS BETWEEN
                    2 PRECEDING
                    AND CURRENT ROW
            ) AS temp_roll_mean_3,

            AVG(temperature)
            OVER (
                weather_window
                ROWS BETWEEN
                    5 PRECEDING
                    AND CURRENT ROW
            ) AS temp_roll_mean_6,

            -- =========================================
            -- RAIN FLAG
            -- =========================================

            CASE
                WHEN precipitation > 0
                THEN 1
                ELSE 0
            END
            AS rain_flag,

            -- =========================================
            -- INTERACTION FEATURE
            -- =========================================

            temperature
            *
            CASE
                WHEN precipitation > 0
                THEN 1
                ELSE 0
            END
            AS temp_x_rain,

            -- =========================================
            -- SUPERVISED TARGET
            -- =========================================

            LEAD(
                temperature, 1
            )
            OVER weather_window
            AS target

        FROM latest_forecast

        WINDOW weather_window AS (
            PARTITION BY city
            ORDER BY forecast_datetime
        )
    ),

    inserted AS (

        INSERT INTO analytics.weather_features (

            city,
            latitude,
            longitude,
            forecast_datetime,

            temperature,
            precipitation,

            hour,
            hour_sin,
            hour_cos,

            temp_lag_1,
            temp_lag_2,
            temp_lag_3,
            temp_lag_6,

            precip_lag_1,
            precip_lag_2,

            temp_diff,

            temp_roll_mean_3,
            temp_roll_mean_6,

            rain_flag,
            temp_x_rain,

            target
        )

        SELECT *

        FROM feature_base

        WHERE
            temp_lag_6 IS NOT NULL
            AND target IS NOT NULL

        RETURNING 1
    )

    SELECT COUNT(*)
    FROM inserted;
    """

    cursor.execute(truncate_sql)

    cursor.execute(insert_sql)

    rows = cursor.fetchone()[0]

    conn.commit()

    logger.info(
        "ANALYTICS weather features "
        "load completed. Rows: %s",
        rows,
    )

    cursor.close()
    conn.close()

    return rows