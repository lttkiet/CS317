"""Vertex AI Pipeline definition for weekly model retraining.

This pipeline triggers a Dataproc Serverless batch job that:
1. Queries mlops_training_view for the latest settled data (90 days)
2. Trains a new Random Forest model with class weight balancing
3. Evaluates against held-out test set (ROC-AUC, PR-AUC)
4. Promotes model if evaluation gate passes

Schedule: Cloud Scheduler cron `0 2 * * 1` (every Monday at 2:00 AM UTC)
"""
from datetime import datetime
from google_cloud_pipeline_components.v1 import dataproc
from kfp import dsl


@dsl.pipeline(
    name="fraud-model-retraining",
    description="Weekly retraining of fraud detection Random Forest model",
)
def retraining_pipeline(
    project_id: str = "",
    region: str = "us-central1",
    model_bucket: str = "",
    staging_bucket: str = "",
    training_sa: str = "",
):
    """Main pipeline definition."""

    pipeline_root = f"gs://{project_id}-fraud-pipeline-root"

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # ── Step 1: Dataproc Serverless batch for training ────────────────
    training_task = dataproc.DataprocPySparkBatchOp(
        project=project_id,
        location=region,
        batch_id=f"fraud-retrain-{timestamp}",
        args=[
            f"--project_id={project_id}",
            f"--model_path=gs://{model_bucket}/latest_rf_model",
        ],
        pyspark_batch_main_python_file_uri=f"gs://{staging_bucket}/training/train.py",
        pyspark_batch_jar_file_uris=[
            "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12.jar",
        ],
        pyspark_batch_properties={
            "spark.executor.instances": "4",
            "spark.executor.memory": "8g",
            "spark.executor.cores": "2",
            "spark.sql.shuffle.partitions": "200",
        },
        service_account=training_sa,
    )

    training_task.set_caching_options(False)

    return {"training_output": training_task.output}


def compile_pipeline(output_path: str = "pipeline/pipeline.json"):
    """Compile the pipeline to JSON for Vertex AI upload."""
    import kfp.compiler

    compiler = kfp.compiler.Compiler()
    compiler.compile(
        pipeline_func=retraining_pipeline,
        package_path=output_path,
    )
    print(f"Pipeline compiled to {output_path}")


if __name__ == "__main__":
    compile_pipeline()
