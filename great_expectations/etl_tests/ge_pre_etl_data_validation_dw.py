import great_expectations as gx

context = gx.get_context(context_root_dir="/opt/great_expectations/gx/")

context.get_expectation_suite("pre_etl_sales_view_validation_suite")
context.get_expectation_suite("pre_etl_posts_table_validation_suite")
context.get_expectation_suite("pre_etl_users_table_validation_suite")

context.run_checkpoint(checkpoint_name="pre_etl_sales_view_validation_checkpoint")
context.run_checkpoint(checkpoint_name="pre_etl_users_table_validation_checkpoint")
context.run_checkpoint(checkpoint_name="pre_etl_posts_table_validation_checkpoint")


context.open_data_docs()