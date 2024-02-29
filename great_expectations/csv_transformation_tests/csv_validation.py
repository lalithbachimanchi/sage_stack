import great_expectations as gx


context = gx.get_context(project_root_dir="/opt/great_expectations/")


if context.get_expectation_suite("CSV_Validation"):
    context.get_expectation_suite("CSV_Validation")
else:
    context.add_expectation_suite("CSV_Validation")

context.run_checkpoint(checkpoint_name="source-csv-checkpoint")

context.open_data_docs()
