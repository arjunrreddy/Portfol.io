from datetime import timedelta
from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from python_scripts.creds import mint_info

from python_scripts.mint_python import (
    gathering_mint_data,
    deleting_previous_s3,
    converting_data_to_dataframe,
    uploading_csv_into_bucket,
    getting_column_names,
    upload_data_to_postgresql,
)


default_args = {
    # "owner": "airflow",
    # "depends_on_past": False,
    # "email": ["airflow@example.com"],
    # "email_on_failure": False,
    # "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=3),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # "trigger_rule": "all_success",
    "start_date": datetime(2022, 1, 1),
}


dag =  DAG(
    "extraction_and_loading_dag",
    default_args=default_args,
    description="Testing Python",
    schedule_interval="15,45 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='gathering_mint_data',
    python_callable= gathering_mint_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='deleting_previous_s3',
    python_callable= deleting_previous_s3,
    dag=dag,
)

t3 = PythonOperator(
    task_id='converting_data_to_dataframe',
    python_callable= converting_data_to_dataframe,
    dag=dag,
)

t4 = PythonOperator(
    task_id='uploading_csv_into_bucket',
    python_callable= uploading_csv_into_bucket,
    dag=dag,
)

t5 = PythonOperator(
    task_id='getting_column_names',
    python_callable= getting_column_names,
    dag=dag,
)

t6 = PythonOperator(
    task_id='upload_data_to_postgresql',
    python_callable= upload_data_to_postgresql,
    dag=dag,
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6
