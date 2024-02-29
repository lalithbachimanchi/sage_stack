import great_expectations as gx
import os

#
# destination_db_host=os.environ['POSTGRES_HOST_NAME']
# destination_db_database=os.environ['POSTGRES_DATABASE']
# destination_db_port=os.environ['POSTGRES_PORT']
# destination_db_user_name=os.environ['POSTGRES_USER']
# destination_db_password=os.environ['POSTGRES_PASSWORD']
#
#
# source_db_host=os.environ['MYSQL_HOST_NAME']
# source_db_database=os.environ['MYSQL_DATABASE']
# source_db_port=os.environ['MYSQL_PORT']
# source_db_user_name=os.environ['MYSQL_USER']
# source_db_password=os.environ['MYSQL_PASSWORD']
#
#
# postgres_connection_url = f"postgresql://{destination_db_user_name}:{destination_db_password}@{destination_db_host}:{destination_db_port}/{destination_db_database}"
# mysql_connection_url = f"mysql+pymysql://{source_db_user_name}:{source_db_password}@{source_db_host}:{source_db_port}/{source_db_database}"


context = gx.get_context(project_root_dir="/opt/great_expectations/")

# if context.get_expectation_suite("Data_Migration_Pre_Transformation_Validation"):

context.get_expectation_suite("Data_Migration_Pre_Transformation_Validation_Users")
context.get_expectation_suite("Data_Migration_Pre_Transformation_Validation_Posts")
context.get_expectation_suite("Data_Migration_Pre_Transformation_Validation_Usersmeta")
context.get_expectation_suite("Data_Migration_Pre_Transformation_Validation_Postsmeta")
context.get_expectation_suite("Data_Migration_Pre_Transformation_Validation_commerce_order_items")
context.get_expectation_suite("Data_Migration_Pre_Transformation_Validation_commerce_order_itemsmeta")

#

context.run_checkpoint(checkpoint_name="data_migration_pre_transformation_validation_users_checkpoint")
context.run_checkpoint(checkpoint_name="data_migration_pre_transformation_validation_posts_checkpoint")
context.run_checkpoint(checkpoint_name="data_migration_pre_transformation_validation_usersmeta_checkpoint")

context.run_checkpoint(checkpoint_name="data_migration_pre_transformation_validation_postsmeta_checkpoint")
context.run_checkpoint(checkpoint_name="data_migration_pre_transformation_validation_commerce_order_items_checkpoint")
tt =  context.run_checkpoint(checkpoint_name="data_migration_pre_transformation_validation_commerce_order_itemsmeta_checkpoint")


assert tt['success'], "errrrrrrr"


context.open_data_docs()


# # else:
# context.add_expectation_suite("Data_Migration_Pre_Transformation_Validation_Users")
# context.add_expectation_suite("Data_Migration_Pre_Transformation_Validation_Posts")
# context.add_expectation_suite("Data_Migration_Pre_Transformation_Validation_Usersmeta")
# context.add_expectation_suite("Data_Migration_Pre_Transformation_Validation_Postsmeta")
# context.add_expectation_suite("Data_Migration_Pre_Transformation_Validation_commerce_order_items")
# context.add_expectation_suite("Data_Migration_Pre_Transformation_Validation_commerce_order_itemsmeta")
# # context.get_expectation_suite("Data_Migration_DB_Check_Validation")
#
# # mysql_data_source = context.sources.add_sql(
# # name="mysql_data_source", connection_string=mysql_connection_url
# # # )
# #
# source_db_datasource = context.get_datasource("mysql_data_source")
#
# # source_users_data_asset = source_db_datasource.add_table_asset(name="source_users_data_asset", table_name="genaidb.users")
# # source_posts_data_asset = source_db_datasource.add_table_asset(name="source_posts_data_asset", table_name="genaidb.posts")
# # source_usermeta_data_asset = source_db_datasource.add_table_asset(name="source_usermeta_data_asset", table_name="genaidb.usermeta")
# # source_postmeta_data_asset = source_db_datasource.add_table_asset(name="source_postmeta_data_asset", table_name="genaidb.postmeta")
# # source_commerce_order_items_data_asset = source_db_datasource.add_table_asset(name="source_commerce_order_items_data_asset", table_name="genaidb.commerce_order_items")
# # source_commerce_order_itemsmeta_data_asset = source_db_datasource.add_table_asset(name="source_commerce_order_itemsmeta_data_asset", table_name="genaidb.commerce_order_itemsmeta")
#
#
# source_users_batch_request = source_db_datasource.get_asset("source_users_data_asset").build_batch_request(batch_slice="[-100:]")
# source_posts_batch_request = source_db_datasource.get_asset("source_posts_data_asset").build_batch_request(batch_slice="[-100:]")
# source_usermeta_batch_request = source_db_datasource.get_asset("source_usermeta_data_asset").build_batch_request(batch_slice="[-100:]")
# source_postmeta_batch_request = source_db_datasource.get_asset("source_postmeta_data_asset").build_batch_request(batch_slice="[-100:]")
# source_commerce_order_items_batch_request = source_db_datasource.get_asset("source_commerce_order_items_data_asset").build_batch_request(batch_slice="[-100:]")
# source_commerce_order_itemsmeta_batch_request = source_db_datasource.get_asset("source_commerce_order_itemsmeta_data_asset").build_batch_request(batch_slice="[-100:]")
#
#
#
#
# source_users_validator = context.get_validator(
# batch_request=source_users_batch_request,
# expectation_suite_name="Data_Migration_Pre_Transformation_Validation_Users",
# )
#
# source_users_validator.expect_column_values_to_not_be_null("user_id")
# source_users_validator.expect_column_values_to_not_be_null("user_email")
#
# source_users_validator.save_expectation_suite()
#
#
# source_posts_validator = context.get_validator(
# batch_request=source_posts_batch_request,
# expectation_suite_name="Data_Migration_Pre_Transformation_Validation_Posts",
# )
#
# source_posts_validator.expect_column_values_to_not_be_null("post_id")
# source_posts_validator.expect_column_values_to_not_be_null("post_author")
#
# source_posts_validator.save_expectation_suite()
#
#
# source_usermeta_validator = context.get_validator(
# batch_request=source_usermeta_batch_request,
# expectation_suite_name="Data_Migration_Pre_Transformation_Validation_Usersmeta",
# )
#
# source_usermeta_validator.expect_column_values_to_not_be_null("umeta_id")
# source_usermeta_validator.expect_column_values_to_not_be_null("user_id")
#
# source_usermeta_validator.save_expectation_suite()
#
#
# source_postmeta_validator = context.get_validator(
# batch_request=source_postmeta_batch_request,
# expectation_suite_name="Data_Migration_Pre_Transformation_Validation_Postsmeta",
# )
#
# source_postmeta_validator.expect_column_values_to_not_be_null("meta_id")
# source_postmeta_validator.expect_column_values_to_not_be_null("post_id")
#
# source_postmeta_validator.save_expectation_suite()
#
#
# source_commerce_order_items_validator = context.get_validator(
# batch_request=source_commerce_order_items_batch_request,
# expectation_suite_name="Data_Migration_Pre_Transformation_Validation_commerce_order_items",
# )
#
# source_commerce_order_items_validator.expect_column_values_to_not_be_null("order_item_id")
# source_commerce_order_items_validator.expect_column_values_to_not_be_null("order_id")
#
# source_commerce_order_items_validator.save_expectation_suite()
#
#
#
# source_commerce_order_itemsmeta_validator = context.get_validator(
# batch_request=source_commerce_order_itemsmeta_batch_request,
# expectation_suite_name="Data_Migration_Pre_Transformation_Validation_commerce_order_itemsmeta",
# )
#
# source_commerce_order_itemsmeta_validator.expect_column_values_to_not_be_null("meta_id")
# source_commerce_order_itemsmeta_validator.expect_column_values_to_not_be_null("order_item_id")
#
# source_commerce_order_itemsmeta_validator.save_expectation_suite()
#
#
# source_users_checkpoint = context.add_or_update_checkpoint(
#     name="data_migration_pre_transformation_validation_users_checkpoint",
#     validator=source_users_validator,
#     expectation_suite_name="Data_Migration_Pre_Transformation_Validation_Users")
# source_users_checkpoint_result = source_users_checkpoint.run()
#
#
# source_posts_checkpoint = context.add_or_update_checkpoint(
#     name="data_migration_pre_transformation_validation_posts_checkpoint",
#     validator=source_posts_validator,
#     expectation_suite_name="Data_Migration_Pre_Transformation_Validation_Posts")
# source_posts_checkpoint_result = source_posts_checkpoint.run()
#
#
#
# source_usersmeta_checkpoint = context.add_or_update_checkpoint(
#     name="data_migration_pre_transformation_validation_usersmeta_checkpoint",
#     validator=source_usermeta_validator,
#     expectation_suite_name="Data_Migration_Pre_Transformation_Validation_Usersmeta")
# source_usermeta_checkpoint_result = source_usersmeta_checkpoint.run()
#
#
# source_postsmeta_checkpoint = context.add_or_update_checkpoint(
#     name="data_migration_pre_transformation_validation_postsmeta_checkpoint",
#     validator=source_postmeta_validator,
#     expectation_suite_name="Data_Migration_Pre_Transformation_Validation_Postsmeta")
# source_postsmeta_checkpoint_result = source_postsmeta_checkpoint.run()
#
#
# source_commerce_order_items_checkpoint = context.add_or_update_checkpoint(
#     name="data_migration_pre_transformation_validation_commerce_order_items_checkpoint",
#     validator=source_commerce_order_items_validator,
#     expectation_suite_name="Data_Migration_Pre_Transformation_Validation_commerce_order_items")
# source_commerce_order_items_checkpoint_result = source_commerce_order_items_checkpoint.run()
#
#
#
# source_commerce_order_itemsmeta_checkpoint = context.add_or_update_checkpoint(
#     name="data_migration_pre_transformation_validation_commerce_order_itemsmeta_checkpoint",
#     validator=source_commerce_order_itemsmeta_validator,
#     expectation_suite_name="Data_Migration_Pre_Transformation_Validation_commerce_order_itemsmeta")
# source_commerce_order_itemsmeta_checkpoint_result = source_commerce_order_itemsmeta_checkpoint.run()
#
#
#
# context.build_data_docs()