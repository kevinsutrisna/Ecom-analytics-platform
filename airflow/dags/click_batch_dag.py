from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from data_quality_checks import check_click_data_quality


default_args = {
    "owner": "kevin",
}

with DAG(
    dag_id="click_batch_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["spark", "batch"],
) as dag:

    run_spark_click_job = BashOperator(
        task_id="run_spark_click_batch",
        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 \
        /opt/spark/jobs/batch_click_agg.py
        """,
    )

data_quality_check = PythonOperator(
    task_id="data_quality_check",
    python_callable=check_click_data_quality,
)

run_spark_click_job >> data_quality_check