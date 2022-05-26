from datetime import timedelta
from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator


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
    "dbt_run",
    default_args=default_args,
    description="Running DBT Model",
    schedule_interval="0 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
)

t1 = BashOperator(
    task_id='run_dbt',
    bash_command='dbt run',
    cwd='/dbt/',
    dag=dag,
)

t1
