-- PostgreSQL schema for local BigQuery substitute

-- Table: streaming_predictions
CREATE TABLE IF NOT EXISTS streaming_predictions (
    TRANSACTION_ID                      BIGINT      NOT NULL,
    TX_DATETIME                         TIMESTAMPTZ NOT NULL,
    CUSTOMER_ID                         BIGINT      NOT NULL,
    TERMINAL_ID                         BIGINT      NOT NULL,
    TX_AMOUNT                           DOUBLE PRECISION NOT NULL,
    TX_TIME_SECONDS                     BIGINT,
    TX_TIME_DAYS                        BIGINT,
    TX_DURING_WEEKEND                   BIGINT,
    TX_DURING_NIGHT                     BIGINT,
    CUSTOMER_ID_NB_TX_1DAY_WINDOW       BIGINT,
    CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW  DOUBLE PRECISION,
    CUSTOMER_ID_NB_TX_7DAY_WINDOW       BIGINT,
    CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW  DOUBLE PRECISION,
    CUSTOMER_ID_NB_TX_30DAY_WINDOW      BIGINT,
    CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW DOUBLE PRECISION,
    TERMINAL_ID_NB_TX_1DAY_WINDOW       BIGINT,
    TERMINAL_ID_RISK_1DAY_WINDOW        DOUBLE PRECISION,
    TERMINAL_ID_NB_TX_7DAY_WINDOW       BIGINT,
    TERMINAL_ID_RISK_7DAY_WINDOW        DOUBLE PRECISION,
    TERMINAL_ID_NB_TX_30DAY_WINDOW      BIGINT,
    TERMINAL_ID_RISK_30DAY_WINDOW       DOUBLE PRECISION,
    FRAUD_PROBABILITY                   DOUBLE PRECISION NOT NULL,
    IS_FRAUD_PREDICTED                  BIGINT      NOT NULL,
    PREDICTED_AT                        TIMESTAMPTZ NOT NULL,
    MODEL_VERSION                       TEXT
);

CREATE INDEX IF NOT EXISTS idx_sp_predicted_at ON streaming_predictions(PREDICTED_AT);
CREATE INDEX IF NOT EXISTS idx_sp_customer ON streaming_predictions(CUSTOMER_ID);
CREATE INDEX IF NOT EXISTS idx_sp_terminal ON streaming_predictions(TERMINAL_ID);
CREATE INDEX IF NOT EXISTS idx_sp_tx_id ON streaming_predictions(TRANSACTION_ID);

-- Table: ground_truth_labels
CREATE TABLE IF NOT EXISTS ground_truth_labels (
    TRANSACTION_ID      BIGINT      NOT NULL,
    TX_FRAUD            BIGINT      NOT NULL,
    TX_FRAUD_SCENARIO   BIGINT      NOT NULL,
    LABEL_RECEIVED_AT   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gl_tx_id ON ground_truth_labels(TRANSACTION_ID);
CREATE INDEX IF NOT EXISTS idx_gl_received ON ground_truth_labels(LABEL_RECEIVED_AT);

-- Table: model_versions
CREATE TABLE IF NOT EXISTS model_versions (
    version             INTEGER PRIMARY KEY,
    model_path          TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    roc_auc             DOUBLE PRECISION,
    pr_auc              DOUBLE PRECISION,
    precision           DOUBLE PRECISION,
    recall              DOUBLE PRECISION,
    tp                  INTEGER,
    fp                  INTEGER,
    fn                  INTEGER,
    tn                  INTEGER,
    best_threshold      DOUBLE PRECISION,
    hyperparameters     TEXT,
    feature_importances TEXT
);

-- View: mlops_training_view
CREATE OR REPLACE VIEW mlops_training_view AS
SELECT
    sp.*,
    gt.TX_FRAUD,
    gt.TX_FRAUD_SCENARIO
FROM streaming_predictions sp
LEFT JOIN ground_truth_labels gt
    ON sp.TRANSACTION_ID = gt.TRANSACTION_ID
WHERE gt.TX_FRAUD IS NOT NULL;

-- Table: data_drift_report
CREATE TABLE IF NOT EXISTS data_drift_report (
    dt              DATE,
    tx_count        BIGINT,
    mean_amount     DOUBLE PRECISION,
    std_amount      DOUBLE PRECISION,
    pct_night       DOUBLE PRECISION,
    pct_weekend     DOUBLE PRECISION,
    mean_fraud_prob DOUBLE PRECISION,
    predicted_fraud_count BIGINT,
    fraud_rate      DOUBLE PRECISION
);

-- Table: concept_drift_report
CREATE TABLE IF NOT EXISTS concept_drift_report (
    pred_date       DATE,
    evaluated       BIGINT,
    tp              BIGINT,
    fp              BIGINT,
    fn              BIGINT,
    tn              BIGINT,
    precision       DOUBLE PRECISION,
    recall          DOUBLE PRECISION,
    sensitivity     DOUBLE PRECISION,
    specificity     DOUBLE PRECISION
);
