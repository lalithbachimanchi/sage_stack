# import great_expectations as gx
# from great_expectations.data_context import DataContext
#
# # from great_expectations.dataset import SqlAlchemyDataset
# context = gx.get_context()
#
# # create in the first run
# # context.add_expectation_suite("GE_suite")
# #
# # Sql_CONNECTION_STRING = (
# # "mysql+pymysql://root:root@localhost:3307/genaidb"
# # )
# # # create in the first run
# # my_datasource = context.sources.add_sql(
# # name="GE_datasource", connection_string=Sql_CONNECTION_STRING
# # )
#
# datasource = context.get_datasource("GE_datasource")
# # print(datasource)
#
# # create in the first run
# # table_asset = datasource.add_table_asset(name="GE_table_asset", table_name="health_care_data")
# batch_request = datasource.get_asset("GE_table_asset").build_batch_request(batch_slice="[-100:]")
#
#
# validator = context.get_validator(
# batch_request=batch_request,
# expectation_suite_name="GE_suite",
# )
# # print("source table row count",validator.expect_table_row_count_to_equal(value=10000)["result"]['observed_value'])
#
# source_count=validator.expect_table_row_count_to_equal(value=10000)["result"]['observed_value']
# print("source table row count",source_count)
#
# # Pg_CONNECTION_STRING = (
# #     "postgresql://postgres_user:postgres_password@localhost:5433/genaidb"
# # )
# # create in the first run
# # my_datasource = context.sources.add_sql(
# #     name="GE_pg_datasource", connection_string=Pg_CONNECTION_STRING
# # )
#
# datatrg= context.get_datasource("GE_pg_datasource")
# # print(datasource)
#
# # create in the first run
# # table_asset = datatrg.add_table_asset(name="Pg_table_asset", table_name="genaidb.users")
# batch_request = datatrg.get_asset("Pg_table_asset").build_batch_request(batch_slice="[-100:]")
#
# validator = context.get_validator(
#     batch_request=batch_request,
#     expectation_suite_name="GE_suite",
# )
#
#
# Trg_count=validator.expect_table_row_count_to_equal(value=source_count)["result"]['observed_value']
# print("Target table row count",Trg_count)
# # print(validator.expect_table_row_count_to_equal(value=source_count))
# print("Count validation result",validator.expect_table_row_count_to_equal(value=source_count)["success"])
#
# # validator.head()
# validator.expect_column_to_exist("user_login")
# print(validator.expect_column_to_exist("user_login"))
