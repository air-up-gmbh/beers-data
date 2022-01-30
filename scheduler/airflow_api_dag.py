from datetime import timedelta
import time

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

dag_name = 'api_data_scheduled'
args = {
    'owner': 'Jay',
    'start_date': days_ago(2),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'email': 'jsaini.coder@gmail.in',
    'email_on_failure': True,
    'email_on_retry': False,
    'provide_context': True
}

scripts_path = '/home/jay/workspace/air-up/beers-data/fetch_data.py'

dag = DAG(
    dag_id=dag_name,
    default_args=args,
    dagrun_timeout=timedelta(minutes=120),
    schedule_interval="0 3 * * *"
)


def halt_task():
    """Halt the dag execution by some seconds """
    time.sleep(2)
    print('Halt completed')


halt_task = PythonOperator(
    task_id='halt_task',
    python_callable=halt_task,
    dag=dag
)

api_data = BashOperator(task_id='api_data', dag=dag, xcom_push=False,
                        bash_command="python3 {}".format(scripts_path))

halt_task >> api_data
