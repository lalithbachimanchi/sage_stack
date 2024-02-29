import os
import great_expectations as gx
from great_expectations.data_context import DataContext
# from great_expectations.datasource import Datasource
# from great_expectations.execution_engine import SqlAlchemyExecutionEngine

# Get environment variables
source_db_host = os.environ['MYSQL_HOST_NAME']
source_db_database = os.environ['MYSQL_DATABASE']
source_db_port = os.environ['MYSQL_PORT']
source_db_user_name = os.environ['MYSQL_USER']
source_db_password = os.environ['MYSQL_PASSWORD']

# Set up your data context
context = DataContext()

# Create a SQLAlchemyExecutionEngine
# engine = SqlAlchemyExecutionEngine(
#     connection_string=f"mysql+pymysql://{source_db_user_name}:{source_db_password}@{source_db_host}:{source_db_port}/{source_db_database}"
# )

mysql_connection_url = f"mysql+pymysql://{source_db_user_name}:{source_db_password}@{source_db_host}:{source_db_port}/{source_db_database}"


# Configure your data source
datasource_config = {
    "name": "mysql_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": mysql_connection_url
    }
}
context.add_datasource(**datasource_config)

# Create and configure expectation suite
suite_configs = [
    {
        # "name": "users_suite",
        "batch_kwargs": {
            "table": "users"
        },
        "expectation_suite_name": "users_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "user_id"
                }
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "user_status"
                }
            },
            # Add more expectations as needed
        ]
    },
    {
        # "name": "posts_suite",
        "batch_kwargs": {
            "table": "posts"
        },
        "expectation_suite_name": "posts_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "post_id"
                }
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "post_author"
                }
            },
            # Add more expectations as needed
        ]
    },
    # Add more tables as needed
]

for suite_config in suite_configs:
    context.add_expectation_suite(**suite_config)

for suite_config in suite_configs:
    batch = context.get_batch("mysql_datasource", batch_kwargs=suite_config["batch_kwargs"])
    result = context.run_validation_operator("action_list_operator", [batch], suite_config["expectation_suite_name"])

# Generate HTML reports
context.build_data_docs()
