import great_expectations as gx


context = gx.get_context(project_root_dir="/opt/great_expectations/")

context.get_expectation_suite("Data_Migration_DB_Check_Validation")

context.run_checkpoint("data_migration_destination_db_checkpoint")

context.open_data_docs()