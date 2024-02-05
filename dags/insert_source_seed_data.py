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

insert_posts_data_bash = """
cd /opt/data/source_seed_data
python3 insert_seed_data_posts.py
"""

insert_commerce_orders_data_bash = """
cd /opt/data/source_seed_data
python3 insert_seed_data_commerce_order.py
"""

with DAG(
    dag_id = 'Insert_Seed_Data_into_Source_Database',
    description = 'Insert Seed Data to Source Database Mysql',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['sql', 'bash', 'seed data']
) as dag:

    insert_users_data = BashOperator(
        task_id = 'insert_users_data',
        bash_command = insert_users_data_bash,
    )

    insert_posts_data = BashOperator(
        task_id='insert_posts_data',
        bash_command=insert_posts_data_bash,
    )

    insert_commerce_orders_data = BashOperator(
        task_id='insert_commerce_orders_data',
        bash_command=insert_commerce_orders_data_bash,
    )

insert_users_data >> insert_posts_data >> insert_commerce_orders_data