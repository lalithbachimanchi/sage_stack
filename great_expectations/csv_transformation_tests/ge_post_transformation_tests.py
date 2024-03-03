import great_expectations as gx


context = gx.get_context(context_root_dir="/opt/great_expectations/gx/")


context.get_expectation_suite("csv_transformation_data_validation_suite")
result = context.run_checkpoint(checkpoint_name="csv_transformation_data_validation_checkpoint")

context.open_data_docs()

assert result['success'], "Tests Failed. Please review Great Expectations Test Report"
