-- Model Performance Summary (for Looker Studio dashboard)
-- Returns latest evaluation metrics for the model versions table

SELECT
  version,
  created_at,
  roc_auc,
  pr_auc,
  precision,
  recall,
  (tp + fp + fn + tn) AS total_evaluated,
  tp,
  fp,
  fn,
  tn,
  SAFE_DIVIDE(tp, tp + fn) AS recall_rate,
  SAFE_DIVIDE(tp, tp + fp) AS precision_rate
FROM {project_id}.fraud_detection.model_versions
ORDER BY created_at DESC
LIMIT 20
