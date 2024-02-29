import great_expectations as gx
import os

#
# destination_db_host=os.environ['POSTGRES_HOST_NAME']
# destination_db_database=os.environ['POSTGRES_DATABASE']
# destination_db_port=os.environ['POSTGRES_PORT']
# destination_db_user_name=os.environ['POSTGRES_USER']
# destination_db_password=os.environ['POSTGRES_PASSWORD']
#
#
# source_db_host=os.environ['MYSQL_HOST_NAME']
# source_db_database=os.environ['MYSQL_DATABASE']
# source_db_port=os.environ['MYSQL_PORT']
# source_db_user_name=os.environ['MYSQL_USER']
# source_db_password=os.environ['MYSQL_PASSWORD']
#
#
# postgres_connection_url = f"postgresql://{destination_db_user_name}:{destination_db_password}@{destination_db_host}:{destination_db_port}/{destination_db_database}"
# mysql_connection_url = f"mysql+pymysql://{source_db_user_name}:{source_db_password}@{source_db_host}:{source_db_port}/{source_db_database}"


context = gx.get_context(project_root_dir="/opt/great_expectations/")

# if context.get_expectation_suite("Data_Migration_DB_Check_Validation"):
context.get_expectation_suite("Data_Migration_DB_Check_Validation")

context.run_checkpoint(checkpoint_name="data_migration_source_db_checkpoint")
context.run_checkpoint("data_migration_destination_db_checkpoint")

context.open_data_docs()


# else:
# context.add_expectation_suite("Data_Migration_DB_Check_Validation")


# mysql_data_source = context.sources.add_sql(
# name="mysql_data_source", connection_string=mysql_connection_url
# )
#
# source_db_datasource = context.get_datasource("mysql_data_source")
#
# # source_table_asset = source_db_datasource.add_table_asset(name="source_health_care_data_asset", table_name="genaidb.health_care_data")
# source_batch_request = source_db_datasource.get_asset("source_health_care_data_asset").build_batch_request(batch_slice="[-100:]")
#
#
# source_validator = context.get_validator(
# batch_request=source_batch_request,
# expectation_suite_name="Data_Migration_DB_Check_Validation",
# )
#
# print(source_validator.head())
#
# source_validator.expect_column_to_exist("external_account_id")
# source_validator.expect_column_to_exist("original_balance")
#
# source_validator.save_expectation_suite()
#
#
# # postgres_datasource = context.sources.add_sql(
# #     name="postgres_datasource", connection_string=postgres_connection_url
# # )
#
# destination_db_datasource= context.get_datasource("postgres_datasource")
#
# # destination_table_asset = destination_db_datasource.add_table_asset(name="destination_health_care_data_asset", table_name="genaidb.health_care_data")
# destination_batch_request = destination_db_datasource.get_asset("destination_health_care_data_asset").build_batch_request(batch_slice="[-100:]")
#
# destination_validator = context.get_validator(
#     batch_request=destination_batch_request,
#     expectation_suite_name="Data_Migration_DB_Check_Validation",
# )
#
# print(destination_validator.head())
#
# destination_validator.expect_column_to_exist("external_account_id")
# destination_validator.expect_column_to_exist("original_balance")
#
# destination_validator.save_expectation_suite()
#
# source_checkpoint = context.add_or_update_checkpoint(
#     name="data_migration_source_db_checkpoint",
#     validator=source_validator,
#     expectation_suite_name="Data_Migration_DB_Check_Validation")
#
#
# destination_checkpoint = context.add_or_update_checkpoint(
#     name="data_migration_destination_db_checkpoint",
#     validator=destination_validator,
#     expectation_suite_name="Data_Migration_DB_Check_Validation")
#
# source_checkpoint_result = source_checkpoint.run()
# destination_checkpoint_result = destination_checkpoint.run()
#
# context.build_data_docs()