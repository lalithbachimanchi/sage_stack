import great_expectations as gx
# from great_expectations.dataset import SqlAlchemyDataset

context = gx.get_context()

# create in the first run
context.create_expectation_suite("GE_suite")



Sql_CONNECTION_STRING = (
"mysql+pymysql://root:root@localhost:3307/genaidb"
)
# create in the first run
my_datasource = context.sources.add_sql(
name="GE_datasource", connection_string=Sql_CONNECTION_STRING
)


datasource = context.get_datasource("GE_datasource")
# print(datasource)

# create in the first run
table_asset = datasource.add_table_asset(name="GE_table_asset", table_name="health_care_data")
batch_request = datasource.get_asset("GE_table_asset").build_batch_request(batch_slice="[-100:]")


validator = context.get_validator(
batch_request=batch_request,
expectation_suite_name="GE_suite",
)

validator.head()
# expectation_validation_result =validator.expect_column_values_to_not_be_null(column="Emp_id")
# print(expectation_validation_result)

# Data specific check
validator.expect_column_values_to_not_be_null(column="account_status")
print(validator.expect_column_values_to_not_be_null(column="account_status"))

# Schema specific check
validator.expect_column_to_exist("origination_date")
print(validator.expect_column_to_exist("origination_date"))

#data type check

validator.expect_column_values_to_be_of_type(
column="first_name",
type_='VARCHAR',
mostly=0.95# Adjust the "mostly" parameter based on your data
)

print(validator.expect_column_values_to_be_of_type(
column="first_name",
type_='VARCHAR',
mostly=0.95# Adjust the "mostly" parameter based on your data
))



checkpoint = context.add_or_update_checkpoint(
 name="my_checkpoint",
 validations=[
 {
 "batch_request": batch_request,
 "expectation_suite_name": "GE_suite",
 },
],
)
checkpoint_result = checkpoint.run()
# assert checkpoint_result["success"] is True
validator.save_expectation_suite()

retrieved_checkpoint = context.get_checkpoint(name="my_checkpoint")

data_assistant_result = context.assistants.onboarding.run(
batch_request=batch_request
)
data_assistant_result.plot_metrics()

context.save_expectation_suite(data_assistant_result.get_expectation_suite("GE_suite"))
context.build_data_docs()