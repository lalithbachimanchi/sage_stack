from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
	'owner' : 'lalith',
}

# check_source_db_connection_bash = """
# cd /opt/spark/data_migration
# python3 check_source_db_connection.py
# """
#
# check_destination_db_connection_bash = """
# cd /opt/spark/data_migration
# python3 check_destination_db_connection.py
# """
#
# great_expectations_check_db_connection_bash = """
# cd /opt/great_expectations/data_migration_tests
# python3 check_ge_db_connection.py
# """

great_expectations_data_sanity_bash = """
cd /opt/great_expectations/data_migration_tests
python3 ge_pre_etl_data_validation.py
"""

data_migration_task_bash = """
cd /opt/spark/data_migration
python3 mysql_to_postgres_data_migration.py
"""


# great_expectations_run_tests_bash = """
# cd /opt/great_expectations/data_migration_tests
# python3 ge_post_tranformation_validation.py
# """

data_validation_framework_tests_bash = """
cd /opt/data_validation_framework/
python3 main.py --TEST_CASE_KEYS=1,2,3,4,5,6,7,8 --DAG_ID=data-migration-from-mysql-to-postgres
"""

with DAG(
    dag_id = 'data-migration-from-mysql-to-postgres',
    description = 'Migrate Data from Mysql to Postgres',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = None,
    tags = ['mysql', 'postgres', 'data quality', 'spark', 'data migration']
) as dag:

    great_expectations_sanity_check = BashOperator(
        task_id='stage1-pre-data-migration-validations',
        bash_command=great_expectations_data_sanity_bash,
    )

    data_migration_task = BashOperator(
        task_id='stage2-data-migration-task',
        bash_command=data_migration_task_bash,
    )



    data_validation_framework_tests = BashOperator(
        task_id='stage3-post-data-migration-validations',
        bash_command=data_validation_framework_tests_bash,
    )


great_expectations_sanity_check >> data_migration_task >> data_validation_framework_tests