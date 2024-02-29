from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
	'owner' : 'admin',
}

check_db_connection_bash = """
cd /opt/spark/spark_jobs/csv_to_db
python3 check_db_connection.py
"""

great_expectations_check_db_connection_bash = """
cd /opt/great_expectations/csv_transformation_tests
python3 check_ge_db_connection.py
"""

great_expectations_data_sanity_bash = """
cd /opt/great_expectations/csv_transformation_tests
python3 csv_validation.py
"""

spark_data_load_bash = """
cd /opt/spark/spark_jobs/csv_to_db
python3 transform_data_from_csv.py
"""


great_expectations_run_tests_bash = """
cd /opt/great_expectations/csv_transformation_tests
python3 ge_post_transformation_tests.py
"""


with DAG(
    dag_id = 'csv-into-database',
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

    great_expectations_check_db_connection = BashOperator(
        task_id='great_expectations_check_db_connection',
        bash_command=great_expectations_check_db_connection_bash,
    )

    great_expectations_sanity_check = BashOperator(
        task_id='great_expectations_data_sanity',
        bash_command=great_expectations_data_sanity_bash,
    )

    spark_data_load = BashOperator(
        task_id='transform_load_data_from_csv',
        bash_command=spark_data_load_bash,
    )


    great_expectations_run_tests = BashOperator(
        task_id='great_expectations_run_tests',
        bash_command=great_expectations_run_tests_bash,
    )

check_db_connection >> great_expectations_check_db_connection >> great_expectations_sanity_check\
>> spark_data_load  >> great_expectations_run_tests