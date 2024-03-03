import great_expectations as gx

context = gx.get_context(context_root_dir="/opt/great_expectations/gx/")

context.get_expectation_suite("data_migration_user_table_pre_validation_suite")
context.get_expectation_suite("data_migration_posts_table_pre_validation_suite")
context.get_expectation_suite("data_migration_commerce_order_table_pre_validation_suite")

results = []
results.append(context.run_checkpoint(checkpoint_name="data_migration_user_table_pre_validation_checkpoint"))
results.append(context.run_checkpoint(checkpoint_name="data_migration_posts_table_pre_validation_checkpoint"))
results.append(context.run_checkpoint(checkpoint_name="data_migration_commerce_order_table_pre_validation_checkpoint"))

for each_result in results:
    assert each_result['success'], "Tests Failed. Please review Great Expectations Test Report"

context.open_data_docs()