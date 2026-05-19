from airflow.providers.postgres.hooks.postgres import (
    PostgresHook,
)

import pandas as pd
import joblib
import logging

logger = logging.getLogger(__name__)


def run_prediction(
    postgres_conn_id="postgres_default",
):

    logger.info("Starting prediction pipeline...")

    hook = PostgresHook(
        postgres_conn_id=postgres_conn_id
    )

    engine = hook.get_sqlalchemy_engine()

    # =========================================
    # LOAD ACTIVE MODEL
    # =========================================

    metadata_query = """
    SELECT
        model_path,
        model_version,
        trained_at
    FROM analytics.model_metadata
    WHERE is_active = TRUE
    ORDER BY trained_at DESC
    LIMIT 1;
    """

    metadata_df = pd.read_sql(
        metadata_query,
        engine,
    )

    if metadata_df.empty:
        raise ValueError(
            "Nenhum modelo ativo encontrado."
        )

    model_path = metadata_df.iloc[0]["model_path"]
    model_version = metadata_df.iloc[0]["model_version"]
    trained_at = metadata_df.iloc[0]["trained_at"]

    logger.info(
        "Loading model: %s",
        model_path,
    )

    model = joblib.load(model_path)

    # =========================================
    # LOAD FEATURES
    # =========================================

    query = """
    SELECT
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
        temp_x_rain

    FROM analytics.weather_features
    ORDER BY forecast_datetime;
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        raise ValueError(
            "weather_features vazia."
        )

    # =========================================
    # PREPARE FEATURES
    # =========================================

    prediction_base = df.copy()

    X = df.drop(
        columns=[
            "forecast_datetime",
            "city",
        ]
    )

    # =========================================
    # PREDICT
    # =========================================

    predictions = model.predict(X)

    prediction_base[
        "predicted_temperature"
    ] = predictions

    prediction_base[
        "model_version"
    ] = model_version

    prediction_base[
        "trained_at"
    ] = trained_at

    # =========================================
    # SAVE PREDICTIONS
    # =========================================

    conn = hook.get_conn()
    cursor = conn.cursor()

    inserted = 0

    for _, row in prediction_base.iterrows():

        cursor.execute(
            """
            INSERT INTO
            analytics.weather_predictions (

                city,
                latitude,
                longitude,

                forecast_datetime,

                predicted_temperature,

                model_version

            )
            VALUES (

                %s,
                %s,
                %s,

                %s,

                %s,

                %s

            )

            ON CONFLICT (
                latitude,
                longitude,
                forecast_datetime,
                model_version
            )
            DO NOTHING;
            """,
            (
                row["city"],
                row["latitude"],
                row["longitude"],
                row["forecast_datetime"],
                float(
                    row[
                        "predicted_temperature"
                    ]
                ),
                model_version
            ),
        )

        inserted += 1

    conn.commit()

    cursor.close()
    conn.close()

    logger.info(
        "Predictions saved: %s",
        inserted,
    )

    return inserted