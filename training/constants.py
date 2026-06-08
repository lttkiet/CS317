"""Shared constants for the fraud detection pipeline.

Feature column order MUST be identical in training and inference.
Never reorder these columns.
"""

# Canonical 17-feature column order for VectorAssembler (§5.2)
VECTOR_ASSEMBLER_INPUT_COLS = [
    "tx_amount",
    "tx_time_seconds",
    "tx_time_days",
    "tx_during_weekend",
    "tx_during_night",
    "customer_id_nb_tx_1day_window",
    "customer_id_avg_amount_1day_window",
    "customer_id_nb_tx_7day_window",
    "customer_id_avg_amount_7day_window",
    "customer_id_nb_tx_30day_window",
    "customer_id_avg_amount_30day_window",
    "terminal_id_nb_tx_1day_window",
    "terminal_id_risk_1day_window",
    "terminal_id_nb_tx_7day_window",
    "terminal_id_risk_7day_window",
    "terminal_id_nb_tx_30day_window",
    "terminal_id_risk_30day_window",
]

# Random Forest default hyperparameters (§5.2)
# These are used as fallback; cross-validation searches over a grid.
RF_PARAMS = {
    "numTrees": 100,
    "maxDepth": 10,
    "featureSubsetStrategy": "auto",
    "impurity": "gini",
    "subsamplingRate": 1.0,
    "seed": 42,
    "minInstancesPerNode": 1,
    "minInfoGain": 0.0,
    "maxBins": 32,
}

# Cross-validation grid (§3.4)
# Local (small cluster) uses a reduced grid; GCP uses the full grid.
CV_NUM_FOLDS = 3
CV_PARALLELISM = 1
CV_SEED = 42
CV_METRIC = "areaUnderPR"
CV_GRID_NUM_TREES = [50, 100, 150, 200]
CV_GRID_MAX_DEPTH = [5, 10, 15, 20]
CV_GRID_IMPURITY = ["gini", "entropy"]
CV_REDUCED_NUM_TREES = [50, 100]
CV_REDUCED_MAX_DEPTH = [10, 15]
CV_REDUCED_IMPURITY = ["gini"]

# Threshold tuning range (§5.3)
THRESHOLD_TUNE_MIN = 0.05
THRESHOLD_TUNE_MAX = 0.95
THRESHOLD_TUNE_STEP = 0.05
THRESHOLD_TUNE_METRIC = "f1"  # or "youden"

# Inference column names
LABEL_COL = "tx_fraud"
FEATURES_COL = "features"
PREDICTION_COL = "is_fraud_predicted"
PROBABILITY_COL = "fraud_probability"
RAW_PREDICTION_COL = "rawPrediction"

# Default custom threshold (§5.3) — overwritten by threshold tuning
FRAUD_THRESHOLD = 0.52

# Evaluation gate: promote only if new PR-AUC >= current * 1.01 (§3.4)
EVAL_GATE_MARGIN = 1.01
