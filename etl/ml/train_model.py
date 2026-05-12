from time import timezone

from airflow.providers.postgres.hooks.postgres import (
    PostgresHook,
)

import logging
import os
import joblib
import pandas as pd

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import train_test_split


logger = logging.getLogger(__name__)


def check_new_rows(
    postgres_conn_id="postgres_default",
):

    sql_new_rows = """
    WITH last_training AS (

        SELECT
            MAX(max_forecast_datetime)
            AS last_trained_datetime

        FROM analytics.model_metadata

        WHERE is_active = TRUE
    ),

    new_data AS (

        SELECT
            COUNT(*) AS new_rows,
            MIN(forecast_datetime)
                AS min_new_datetime,
            MAX(forecast_datetime)
                AS max_new_datetime

        FROM analytics.weather_features

        WHERE forecast_datetime >
            COALESCE(
                (
                    SELECT last_trained_datetime
                    FROM last_training
                ),
                '1900-01-01'
            )
    )

    SELECT *
    FROM new_data;
    """

    hook = PostgresHook(
        postgres_conn_id=postgres_conn_id
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(sql_new_rows)

    rows = cursor.fetchone()[0]

    cursor.close()

    return rows


def run_training(
    postgres_conn_id="postgres_default",
):

    logger.info(
        "Starting ML model training..."
    )

    # =========================================
    # CONNECTION
    # =========================================

    hook = PostgresHook(
        postgres_conn_id=postgres_conn_id
    )

    engine = hook.get_sqlalchemy_engine()

    # =========================================
    # CHECK NEW ROWS
    # =========================================

    new_rows = check_new_rows()

    if new_rows < 500:

        logger.info(
            "Menos de 500 novas linhas. "
            "Treino ignorado."
        )

        return

    # =========================================
    # FEATURE STORE QUERY
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
        temp_x_rain,

        target

    FROM analytics.weather_features

    ORDER BY forecast_datetime;
    """

    df = pd.read_sql(
        query,
        engine,
    )

    if df.empty:

        raise ValueError(
            "analytics.weather_features "
            "está vazia."
        )

    logger.info(
        "Dataset carregado com %s linhas",
        len(df),
    )

    # =========================================
    # FEATURES / TARGET
    # =========================================

    X = df.drop(
        columns=[
            "target",
            "forecast_datetime",
            "city",
        ]
    )

    y = df["target"]

    # =========================================
    # TRAIN / TEST SPLIT
    # =========================================

    (
        X_train,
        X_test,
        y_train,
        y_test,
    ) = train_test_split(
        X,
        y,
        test_size=0.2,
        shuffle=False,
    )

    logger.info(
        "Treino: %s linhas | "
        "Teste: %s linhas",
        len(X_train),
        len(X_test),
    )

    # =========================================
    # MODEL
    # =========================================

    model = RandomForestRegressor(
        n_estimators=200,
        random_state=42,
        n_jobs=1,
    )

    # =========================================
    # TRAINING
    # =========================================

    model.fit(
        X_train,
        y_train,
    )

    logger.info(
        "Modelo treinado com sucesso."
    )

    # =========================================
    # PREDICTIONS
    # =========================================

    predictions = model.predict(
        X_test
    )

    # =========================================
    # METRICS
    # =========================================

    mae = mean_absolute_error(
        y_test,
        predictions,
    )

    rmse = mean_squared_error(
        y_test,
        predictions,
    ) ** 0.5

    r2 = r2_score(
        y_test,
        predictions,
    )

    logger.info(
        "MAE: %.4f | "
        "RMSE: %.4f | "
        "R2: %.4f",
        mae,
        rmse,
        r2,
    )

    # =========================================
    # FEATURE IMPORTANCE
    # =========================================

    importance_df = pd.DataFrame(
        {
            "feature": X.columns,
            "importance": (
                model.feature_importances_
            ),
        }
    ).sort_values(
        by="importance",
        ascending=False,
    )

    logger.info(
        "Top 10 features:\n%s",
        importance_df.head(10).to_string(
            index=False
        ),
    )

    # =========================================
    # SAVE MODEL
    # =========================================

    os.makedirs(
        "/opt/airflow/models",
        exist_ok=True,
    )

    from datetime import datetime, timezone

    timestamp = datetime.now(
        timezone.utc).strftime("%Y%m%d_%H%M%S")

    model_path = (
    f"/opt/airflow/models/"
    f"weather_model_{timestamp}.pkl"
    )

    joblib.dump(
        model,
        model_path,
    )

    logger.info(
        "Modelo salvo em: %s",
        model_path,
    )

    # =========================================
    # SAVE METADATA
    # =========================================

    conn = hook.get_conn()
    cursor = conn.cursor()

    # desativa modelo anterior
    cursor.execute(
        """
        UPDATE analytics.model_metadata
        SET is_active = FALSE
        WHERE is_active = TRUE;
        """
    )

    cursor.execute(
        """
        INSERT INTO analytics.model_metadata (

            model_name,
            model_version,
            trained_at,

            training_rows,

            mae,
            rmse,
            r2,

            model_path,

            min_forecast_datetime,
            max_forecast_datetime,

            is_active

        )
        VALUES (

            %s,
            %s,
            NOW(),

            %s,

            %s,
            %s,
            %s,

            %s,

            %s,
            %s,

            TRUE
        );
        """,
        (
            "weather_random_forest",
            "v1",

            len(df),

            float(mae),
            float(rmse),
            float(r2),

            model_path,

            df["forecast_datetime"].min(),
            df["forecast_datetime"].max(),
        ),
    )

    conn.commit()

    logger.info(
        "Metadata do modelo salva com sucesso."
    )

    cursor.close()
    conn.close()

    return {
        "rows": len(df),
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "r2": round(r2, 4),
        "model_path": model_path,
    }
    