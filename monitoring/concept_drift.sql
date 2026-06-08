-- Concept Drift Monitoring Query (§3.5)
-- Runs daily at 6:00 AM UTC via BigQuery Scheduled Query
-- Alerts when recall drops below 0.6 for 3 consecutive days

WITH joined AS (
  SELECT
    p.TRANSACTION_ID,
    p.IS_FRAUD_PREDICTED,
    p.FRAUD_PROBABILITY,
    g.TX_FRAUD,
    DATE(p.PREDICTED_AT) AS pred_date
  FROM {project_id}.fraud_detection.streaming_predictions p
  JOIN {project_id}.fraud_detection.ground_truth_labels g
    ON p.TRANSACTION_ID = g.TRANSACTION_ID
  WHERE g.TX_FRAUD IS NOT NULL
),
daily_metrics AS (
  SELECT
    pred_date,
    COUNT(*) AS evaluated,
    SUM(CASE WHEN IS_FRAUD_PREDICTED = 1 AND TX_FRAUD = 1 THEN 1 ELSE 0 END) AS tp,
    SUM(CASE WHEN IS_FRAUD_PREDICTED = 1 AND TX_FRAUD = 0 THEN 1 ELSE 0 END) AS fp,
    SUM(CASE WHEN IS_FRAUD_PREDICTED = 0 AND TX_FRAUD = 1 THEN 1 ELSE 0 END) AS fn,
    SUM(CASE WHEN IS_FRAUD_PREDICTED = 0 AND TX_FRAUD = 0 THEN 1 ELSE 0 END) AS tn
  FROM joined
  GROUP BY pred_date
)
SELECT
  pred_date,
  evaluated,
  tp,
  fp,
  fn,
  tn,
  SAFE_DIVIDE(tp, tp + fp) AS precision,
  SAFE_DIVIDE(tp, tp + fn) AS recall,
  SAFE_DIVIDE(tp, tp + fn) AS sensitivity,
  SAFE_DIVIDE(tn, tn + fp) AS specificity
FROM daily_metrics
ORDER BY pred_date
