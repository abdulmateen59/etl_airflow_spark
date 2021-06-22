import os
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime, timedelta

spark_master = "spark://spark:7077"
file_path = "/usr/local/spark/resources/data/mysql.csv"


def file_exist_check(**context):
    if os.path.isfile(context['file']):
        return 'True'
    else:
        return 'False'

now = datetime.now()
with DAG(
    dag_id='dag',
    schedule_interval='*/5  * * * *',  # every 5mins
    default_args={
        'owner': 'airflow',
        'retries': '0',
        'retry_delay': timedelta(minutes=10),
        'start_date':  datetime(now.year, now.month, now.day),
    },
    catchup=False) as f:

    start = DummyOperator(task_id='start')

    python_job_mysql = PythonOperator(
        task_id="python_job_mysql",
        python_callable=file_exist_check,
        provide_context=True,
        op_kwargs={"file":file_path}
        )

    spark_job_mysql = SparkSubmitOperator(
        task_id="spark_job_mysql",
        application="/usr/local/spark/app/count.py",
        name='count',
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        application_args=[file_path, "{{ti.xcom_pull(task_ids='python_job_mysql')}}"]
        )

    end = DummyOperator(task_id='end')

start >> python_job_mysql >> spark_job_mysql >> end