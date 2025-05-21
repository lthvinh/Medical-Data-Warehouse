from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


with DAG(
    "spark_submit_test_dag_v4"
    , start_date=datetime.now()
    , schedule=None
    , catchup=False
) as dag:
    start = EmptyOperator(task_id = 'start')

    spark_submit_task = SparkSubmitOperator(
        task_id="run_simple_spark_app",
        application="/opt/airflow/dags/spark_simple_app.py",  
        conn_id="spark_conn",  
        packages='io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4'
    )
    start >> spark_submit_task