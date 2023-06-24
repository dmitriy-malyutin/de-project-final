from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from src.py.get_transactions import get_transactions
from src.py.get_currencies import get_curriences
from src.py.update_mart import update_mart
from airflow.models import Variable


ETL_NAME = 'practicum_final_project'
ETL_DESC = 'Итоговая работа по курсу Data Engineer'
ETL_VERSION = 'v1'
ETL_OWNER = 'Malyutin Dmitriy'
DAG_ID = '{}_{}'.format(ETL_NAME, ETL_VERSION)

envs = {
    "POSTGRS_HOST" : Variable.get('POSTGRS_HOST'),
    "POSTGRS_DB_NAME" : Variable.get('POSTGRS_DB_NAME'),
    "POSTGRS_USER" : Variable.get('POSTGRS_USER'),
    "POSTGRS_PASSWORD" : Variable.get('POSTGRS_PASSWORD'),
    "POSTGRS_PORT" : 6432,
    "VERTICA_HOST" : Variable.get('VERTICA_HOST'),
    "VERTICA_DB" : Variable.get('VERTICA_DB'),
    "VERTICA_USER" : Variable.get('VERTICA_USER'),
    "VERTICA_PASSWORD" : Variable.get('VERTICA_PASSWORD'),
    "VERTICA_PORT" : 5432,
    "execution_date": '{{ execution_date }}'
}

default_args = {
    'owner': ETL_OWNER,
    'depends_on_past': False,
    'catchup': True,
    'start_date': datetime(2022, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=60)
}


with DAG(
    dag_id=DAG_ID,
    description=ETL_DESC,
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['practicum', 'final_project', 'DE']
) as dag:
    
    load_transactions = PythonOperator(
        task_id='load_transactions',
        python_callable=get_transactions,
        op_kwargs=envs,
        dag=dag,
    )

    load_curriences = PythonOperator(
        task_id='load_curriences',
        python_callable=get_curriences,
        op_kwargs=envs,
        dag=dag,
    )

    updates_mart = PythonOperator(
        task_id='update_mart',
        python_callable=update_mart,
        op_kwargs=envs,
        dag=dag,
    )




    [load_transactions, load_curriences] >> updates_mart

    