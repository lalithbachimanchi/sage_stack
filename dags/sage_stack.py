from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
	'owner' : 'admin',
}

check_db_connection_bash = """
cd /root/spark/
python3 check_db_connection.py
"""

spark_data_load_bash = """
cd /root/spark/
python3 sample_spark_job.py
"""

great_expectations_data_sanity_bash = """
cd /root/great_expectations
python3 pre_etl_validation.py
"""

great_expectations_run_tests_bash = """
cd /root/great_expectations
python3 post_tranformation_validation.py
"""


with DAG(
    dag_id = 'SAGE_Stack_CSV_Processing',
    description = 'Our first sage stack trail',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = None,
    tags = [ 'bash', 'dbt','sage_stack']
) as dag:

    check_db_connection = BashOperator(
        task_id = 'check_db_connection',
        bash_command = check_db_connection_bash,
    )

    great_expectations_data_sanity = BashOperator(
        task_id='great_expectations_data_sanity',
        bash_command=great_expectations_data_sanity_bash,
    )

    spark_data_load = BashOperator(
        task_id='spark_data_load',
        bash_command=spark_data_load_bash,
    )


    great_expectations_check_db_connection = BashOperator(
        task_id='great_expectations_check_db_connection',
        bash_command="echo Connection Success!!!!",
    )

    great_expectations_run_tests = BashOperator(
        task_id='great_expectations_run_tests',
        bash_command=great_expectations_run_tests_bash,
    )

check_db_connection >> great_expectations_data_sanity >> spark_data_load >> great_expectations_check_db_connection >> great_expectations_run_tests