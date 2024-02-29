import great_expectations as gx

context = gx.get_context(project_root_dir="/opt/great_expectations/")

context.get_expectation_suite("CSV_Post_Transformation_DB_Check")

context.run_checkpoint(checkpoint_name="csv_post_transformation_db_checkpoint")

context.open_data_docs()
