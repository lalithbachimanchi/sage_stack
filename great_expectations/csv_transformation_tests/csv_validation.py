import great_expectations as gx


context = gx.get_context(context_root_dir="/opt/great_expectations/gx/")


context.get_expectation_suite("csv_source_data_sanity_suite")
result = context.run_checkpoint(checkpoint_name="source_csv_data_sanity_checkpoint")

context.open_data_docs()


assert result['success'], "Tests Failed. Please review Great Expectations Test Report"