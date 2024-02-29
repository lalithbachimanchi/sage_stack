from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
	'owner' : 'lalith',
}


check_db_connection_bash = """
cd /opt/spark/data_migration
python3 check_destination_db_connection.py
"""

great_expectations_check_db_connection_bash = """
cd /opt/great_expectations/etl_tests
python3 check_ge_postgres_db_connection.py
"""

great_expectations_data_sanity_bash = """
cd /opt/great_expectations/etl_tests
python3 ge_pre_etl_data_validation_dw.py
"""

transformation_task_bash = """
cd /opt/spark/etl
python3 transformation.py
"""


data_validation_framework_tests_bash = """
cd /opt/data_validation_framework
python3 main.py --TEST_CASE_KEYS=9,10,11 --DAG_ID=etl-job
"""


with DAG(
    dag_id = 'etl-job',
    description = 'ETL Job',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = None,
    tags = ['sql', 'bash', 'data migration']
) as dag:
    check_db_connection = BashOperator(
        task_id='check_source_db_connection',
        bash_command=check_db_connection_bash,
    )

    great_expectations_check_db_connection = BashOperator(
        task_id='great_expectations_check_db_connection',
        bash_command=great_expectations_check_db_connection_bash,
    )

    great_expectations_sanity_check = BashOperator(
        task_id='great_expectations_data_sanity',
        bash_command=great_expectations_data_sanity_bash,
    )

    transformation_task = BashOperator(
        task_id='transformation_task',
        bash_command=transformation_task_bash,
    )


    data_validation_framework_tests = BashOperator(
        task_id='data_validation_framework_tests',
        bash_command=data_validation_framework_tests_bash,
    )

check_db_connection >> great_expectations_check_db_connection >> great_expectations_sanity_check >> transformation_task >> data_validation_framework_tests