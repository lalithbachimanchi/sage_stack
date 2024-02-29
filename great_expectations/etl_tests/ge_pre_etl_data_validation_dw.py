import great_expectations as gx


context = gx.get_context(project_root_dir="/opt/great_expectations/")

context.get_expectation_suite("Pre_ETL_Sales_View_Validation")

context.get_expectation_suite("Pre_ETL_Source_Users_Table_Validation")

context.get_expectation_suite("Pre_ETL_Source_Posts_Table_Validation")

context.get_expectation_suite("Pre_ETL_Source_Posts_Commerce_Order_Items_Table_Validation")


context.run_checkpoint("sales_view_sanity_checkpoint")
context.run_checkpoint("users_table_data_sanity_checkpoint")
context.run_checkpoint("posts_table_data_sanity_checkpoint")
context.run_checkpoint("coi_table_data_sanity_checkpoint")

context.build_data_docs()