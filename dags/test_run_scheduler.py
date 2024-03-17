from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
	'owner' : 'admin',
}

# check_db_connection_bash = """
# cd /opt/spark/spark_jobs/csv_to_db
# python3 check_db_connection.py
# """
#
# great_expectations_check_db_connection_bash = """
# cd /opt/great_expectations/csv_transformation_tests
# python3 check_ge_db_connection.py
# """


great_expectations_run_tests_bash = """
cd /opt/great_expectations/csv_transformation_tests
python3 ge_post_transformation_tests.py
"""


with DAG(
    dag_id = 'test-run-scheduler',
    description = 'Run Data Tests on Scheduled Intervals',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= "@hourly",
    tags = [ 'bash', 'data_quality_assurance','sage_stack', 'streaming_data_processing', 'scheduled_tests']
) as dag:

    # check_db_connection = BashOperator(
    #     task_id = 'check_db_connection',
    #     bash_command = check_db_connection_bash,
    # )

    # great_expectations_check_db_connection = BashOperator(
    #     task_id='great_expectations_check_db_connection',
    #     bash_command=great_expectations_check_db_connection_bash,
    # )

    # great_expectations_sanity_check = BashOperator(
    #     task_id='stage1-streaming-data-sanity-check',
    #     bash_command=great_expectations_data_sanity_bash,
    # )
    #
    # spark_data_load = BashOperator(
    #     task_id='stage2-transform_load_data_from_csv_into_database',
    #     bash_command=spark_data_load_bash,
    # )


    great_expectations_run_tests = BashOperator(
        task_id='live-streaming-data-validation',
        bash_command=great_expectations_run_tests_bash,
    )

great_expectations_run_tests