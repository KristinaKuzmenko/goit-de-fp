from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

connection_id = 'spark-default'

# define DAG
with DAG(
        'final_project_krystyna_k',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["krystyna_k"]
) as dag:
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application='dags/krystyna_kuzmenko_spark_jobs/landing_to_bronze.py',
        conn_id=connection_id,
        verbose=1,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='dags/krystyna_kuzmenko_spark_jobs/bronze_to_silver.py',
        conn_id=connection_id,
        verbose=1,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='dags/krystyna_kuzmenko_spark_jobs/silver_to_gold.py',
        conn_id=connection_id,
        verbose=1,
    )

    # task dependencies
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
