from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
	'owner' : 'lalith',
}

insert_users_data_bash = """
cd /opt/data/source_seed_data
python3 insert_source_seed_data_users.py
"""


with DAG(
    dag_id = 'insert-seed-data-into-source-database',
    description = 'Insert Seed Data to Source Database Mysql',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = None,
    tags = ['sql', 'bash', 'seed data']
) as dag:

    insert_seed_data = BashOperator(
        task_id = 'insert_seed_data',
        bash_command = insert_users_data_bash,
    )


insert_seed_data