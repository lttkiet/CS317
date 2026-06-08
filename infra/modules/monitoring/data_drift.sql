-- Data Drift Monitoring Query (§3.5)
-- Runs daily at 6:00 AM UTC via BigQuery Scheduled Query
-- Alerts when TX_AMOUNT deviates from 7-day rolling average

SELECT
  DATE(PREDICTED_AT) AS dt,
  COUNT(*) AS tx_count,
  AVG(TX_AMOUNT) AS mean_amount,
  STDDEV(TX_AMOUNT) AS std_amount,
  AVG(TX_DURING_NIGHT) AS pct_night,
  AVG(TX_DURING_WEEKEND) AS pct_weekend,
  AVG(FRAUD_PROBABILITY) AS mean_fraud_prob,
  SUM(IS_FRAUD_PREDICTED) AS predicted_fraud_count,
  SAFE_DIVIDE(
    SUM(IS_FRAUD_PREDICTED),
    COUNT(*)
  ) AS fraud_rate
FROM {project_id}.fraud_detection.streaming_predictions
WHERE DATE(PREDICTED_AT) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
GROUP BY dt
ORDER BY dt
