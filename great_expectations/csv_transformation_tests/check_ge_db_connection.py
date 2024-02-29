import great_expectations as gx

context = gx.get_context(project_root_dir="/opt/great_expectations/")


if context.get_expectation_suite("CSV_Transformation_DB_Check"):
    context.get_expectation_suite("CSV_Transformation_DB_Check")
else:
    context.add_expectation_suite("CSV_Transformation_DB_Check")

context.run_checkpoint(checkpoint_name="source_db_checkpoint")

context.open_data_docs()
