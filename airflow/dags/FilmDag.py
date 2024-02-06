
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from program import ingest_data

dag_id = 'MyDag'

# Configuration de la DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'MyDag',
    default_args=default_args,
    description='Un exemple de DAG dans Apache Airflow',
    schedule_interval=timedelta(days=1),  # Exécuter la DAG tous les jours
)
jar_file_path = 'C:/Users/FOMEKONG SANDRA/Desktop/airflow/dags/main-scala-mnm_2.12-1.0.jar'

# Utilisez BashOperator pour exécuter le job Spark localement
# submit_job = BashOperator(
#     task_id='submit_spark_job',
#     bash_command=f'spark-submit {jar_file_path} arg1 arg2',
#     dag=dag,
# )
spark_task = SparkSubmitOperator(
    task_id='spark_job',
    conn_id='spark_default',
    application='/home/ubuntu/Downloads/mnmcount/scala/target/scala-2.12/main-scala-mnmc_2.12-1.0.jar',  # Chemin vers le fichier JAR Scala
    total_executor_cores='2',
    executor_cores='1',
    executor_memory='2g',
    num_executors='2',
    verbose=1,
    java_class= "main.scala.mnmc.Mnmcount",
    dag=dag,
)
ingest_data_task = PythonOperator(
    task_id='ingest_data_task',
    python_callable=ingest_data,
    dag=dag,
)
ingest_data_task >> spark_task
