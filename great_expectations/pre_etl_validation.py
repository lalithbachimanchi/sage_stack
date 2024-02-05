import great_expectations as gx
import pandas as pd

context = gx.get_context()
context.create_expectation_suite("GE_File_suite")

datasource = context.sources.add_pandas_filesystem(
    name="my_pandas_datasource",
    base_directory="/root/data"
)
path_to_data = r'/root/data/sample_etl_data.csv'
# The batching_regex should max file names in the data_directory
asset = datasource.add_csv_asset(
    name="csv_asset",
)
validator1 = context.sources.pandas_default.read_csv(path_to_data)

# validator1 = pd.read_csv('/Users/lalith/sage_stack/data/sample_etl_data.csv')

batch_request = asset.build_batch_request()

validator = context.get_validator(
    batch_request = batch_request,
    expectation_suite_name = "GE_File_suite",
    validator=validator1
)
print(validator1.head())
validator1.expect_column_values_to_not_be_null("origination_date")
print(validator1.expect_column_values_to_not_be_null("origination_date"))
validator1.expect_column_values_to_be_between("original_balance", 0, 10000)
print(validator1.expect_column_values_to_be_between("original_balance", 0, 10000))
# Schema specific check
validator1.expect_column_to_exist("company_type_id")
print(validator1.expect_column_to_exist("company_type_id"))

validator1.save_expectation_suite()

checkpoint = context.add_or_update_checkpoint(
    name="my_quickstart_checkpoint",
    validator=validator1,
)

checkpoint_result = checkpoint.run()
retrieved_checkpoint = context.get_checkpoint(name="my_quickstart_checkpoint")

data_assistant_result = context.assistants.onboarding.run(
    batch_request=batch_request
)
data_assistant_result.plot_metrics()
#data_assistant_result=data_assistant_result.metrics_by_domain()

context.save_expectation_suite(data_assistant_result.get_expectation_suite("GE_File_suite"))
context.build_data_docs()