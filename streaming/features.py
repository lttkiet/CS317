"""Feature engineering for the fraud detection streaming pipeline.

Implements §4.2 of the design doc:
1. Temporal features (stateless): TX_DURING_WEEKEND, TX_DURING_NIGHT
2. Customer behavioral features (stateful): 1/7/30-day windowed aggregations
3. Terminal risk features (stateful): 1/7/30-day windowed aggregations

Terminal risk features use a 7-day offset to prevent data leakage from
not-yet-arrived ground-truth labels.
"""
from typing import Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_temporal_features(df: DataFrame) -> DataFrame:
    """Add stateless temporal features from TX_DATETIME (§4.2 §1)."""
    return (
        df.withColumn(
            "TX_DURING_WEEKEND",
            F.when(F.dayofweek(F.col("TX_DATETIME")).isin(1, 7), 1).otherwise(0),
        )
        .withColumn(
            "TX_DURING_NIGHT",
            F.when(F.hour(F.col("TX_DATETIME")) <= 6, 1).otherwise(0),
        )
    )


def compute_customer_window_features(
    df: DataFrame, watermark_delay: str = "24 hours"
) -> DataFrame:
    """Compute customer behavioral windowed aggregations (§4.2 §2).

    Returns a DataFrame with the original columns plus:
    - CUSTOMER_ID_NB_TX_{1,7,30}DAY_WINDOW
    - CUSTOMER_ID_AVG_AMOUNT_{1,7,30}DAY_WINDOW

    Uses rangeBetween window functions over TX_TIME_SECONDS for efficiency.
    """
    _SECONDS_PER_DAY = 86400
    windows = {
        "1DAY": (-_SECONDS_PER_DAY, 0),
        "7DAY": (-7 * _SECONDS_PER_DAY, 0),
        "30DAY": (-30 * _SECONDS_PER_DAY, 0),
    }

    for suffix, (start, end) in windows.items():
        w = Window.partitionBy("CUSTOMER_ID").orderBy("TX_TIME_SECONDS").rangeBetween(start, end)
        df = df.withColumn(f"CUSTOMER_ID_NB_TX_{suffix}_WINDOW", F.count("*").over(w))
        df = df.withColumn(f"CUSTOMER_ID_AVG_AMOUNT_{suffix}_WINDOW", F.avg("TX_AMOUNT").over(w))

    return df


def compute_terminal_risk_features(
    df: DataFrame, risk_table: Optional[DataFrame] = None
) -> DataFrame:
    """Compute terminal risk features (§4.2 §3).

    In production, these are pre-computed from a BQ lookup table with 7-day
    offset to prevent data leakage. For the demo, we accept an optional
    risk_table DataFrame to join against.

    If risk_table is None, columns default to 0.0 (cold start).
    """
    risk_cols = [
        "TERMINAL_ID_NB_TX_1DAY_WINDOW",
        "TERMINAL_ID_RISK_1DAY_WINDOW",
        "TERMINAL_ID_NB_TX_7DAY_WINDOW",
        "TERMINAL_ID_RISK_7DAY_WINDOW",
        "TERMINAL_ID_NB_TX_30DAY_WINDOW",
        "TERMINAL_ID_RISK_30DAY_WINDOW",
    ]

    if risk_table is not None:
        df = df.join(risk_table, on="TERMINAL_ID", how="left")
    else:
        for col_name in risk_cols:
            if col_name not in df.columns:
                df = df.withColumn(col_name, F.lit(0.0))

    # Fill any remaining nulls
    df = df.fillna(0.0, subset=[c for c in risk_cols if c in df.columns])

    return df
