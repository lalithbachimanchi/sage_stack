import os
import json
from datetime import datetime, timedelta
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, when, coalesce, lower, to_date

source_db_host=os.environ['MYSQL_HOST_NAME']
source_db_database=os.environ['MYSQL_DATABASE']
source_db_port=os.environ['MYSQL_PORT']
source_db_user_name=os.environ['MYSQL_USER']
source_db_password=os.environ['MYSQL_PASSWORD']

destination_db_host=os.environ['POSTGRES_HOST_NAME']
destination_db_database=os.environ['POSTGRES_DATABASE']
destination_db_port=os.environ['POSTGRES_PORT']
destination_db_user_name=os.environ['POSTGRES_USER']
destination_db_password=os.environ['POSTGRES_PASSWORD']


# Set up Spark session
spark = SparkSession.builder\
    .appName("Transformation").getOrCreate()

# PostgreSQL connection parameters
postgres_url = f"jdbc:postgresql://{destination_db_host}:{destination_db_port}/{destination_db_database}"
postgres_properties = {
    "user": destination_db_user_name,
    "password": destination_db_password,
    "driver": "org.postgresql.Driver",
}

# empty_df = spark.createDataFrame()

with open(os.environ["TABLE_MAPPING_JSON_PATH"], "r") as table_mapping_file:
    table_mapping_dict = json.loads(table_mapping_file.read())

for each_table in list(table_mapping_dict.values()):
    pass




df_list = []

for each_table in list(table_mapping_dict.values()):
    table_df = spark.read.jdbc(url=postgres_url, table=each_table, properties=postgres_properties)
    df_list.append(table_df)



from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("CreateSalesTable") \
    .getOrCreate()

# Load data from existing tables into DataFrames
users_df = spark.read.jdbc(url=postgres_url, table="genaidb.users", properties=postgres_properties)
usermeta_df = spark.read.jdbc(url=postgres_url, table="genaidb.usermeta", properties=postgres_properties)
posts_df = spark.read.jdbc(url=postgres_url, table="genaidb.posts", properties=postgres_properties)
postmeta_df = spark.read.jdbc(url=postgres_url, table="genaidb.postmeta", properties=postgres_properties)
order_items_df = spark.read.jdbc(url=postgres_url, table="genaidb.commerce_order_items", properties=postgres_properties)
order_itemsmeta_df = spark.read.jdbc(url=postgres_url, table="genaidb.commerce_order_itemsmeta", properties=postgres_properties)


# Perform SQL join on user ID column
joined_df = users_df.join(posts_df, users_df.user_id == posts_df.post_author, "inner") \
    .join(order_items_df, users_df.user_id == order_items_df.order_user, "inner")

final_df = joined_df.select(
    col("user_id"), col("user_email"), col("user_url"), col("user_status"), col("display_name"),
    col("post_id"), col("post_author"), col("post_title"), col("post_date"), col("post_excerpt"), col("post_status"),
    col("post_name"),
    col("post_content_filtered"),col("post_type"),
    col("order_item_id"), col("order_item_name"), col("order_item_type"), col("order_id"),

    # col("order_items_df.subscription_period"), col("order_items_df.qualify_rn"), col("order_items_df.SOURCE_SYSTEM"), col("order_items_df.PRODUCT_SEGMENT")
)



# Define the conditions and corresponding values
conditions = [
    (col("user_email").like("%liquidweb.com%"), True),
    (col("user_email").like("%givewp.com%"), True),
    (col("user_email").like("%iconicwp.com%"), True),
    (col("user_email").like("%impress.org%"), True),
    (col("user_email").like("%stellarwp.com%"), True),
    (col("user_email").like("%ithemes.com%"), True),
    (col("user_email").like("%kadencewp.com%"), True),
    (col("user_email").like("%learndash.com%"), True),
    (col("user_email").like("%nexcess.net%"), True),
    (col("user_email").like("%tri.be%"), True),
    (col("user_email").like("%wpbusinessreviews.com%"), True),
    (col("user_email").like("%theeventscalendar.com%"), True)
]

# Apply the conditions using when() and otherwise()
test_email_df = final_df.withColumn("test_email", when(conditions[0][0], conditions[0][1]))
for condition in conditions[1:]:
    test_email_df = test_email_df.withColumn("test_email", when(condition[0], condition[1]).otherwise(False))


conditions = [
    (col("order_item_type").like("%cloud%"), "Cloud")
]

# Apply the conditions using when() and otherwise()
product_seg_df = test_email_df.withColumn("product_segment", when(conditions[0][0], conditions[0][1]).otherwise("Plugin"))


conditions = [
    ((col("post_title") == "LearnDash Cloud") & (col("order_item_name").like("%annual%")), "Annual"),
    ((col("post_title") == "LearnDash Cloud") & (col("order_item_name").like("%monthly%")), "Monthly")
]


# Apply the conditions using when() and otherwise()
subscription_period_df = product_seg_df.withColumn("subscription_period", when(conditions[0][0], conditions[0][1]).otherwise("NA"))



conditions = (
    (col("post_date") > "2022-05-25") &
    (col("user_email") != "latoya+%@theeventscalendar.com") &
    (col("user_email") != "latoya+%@tri.be")
)


qualify_rn_df = subscription_period_df.withColumn("qualify_rn", when(conditions, True).otherwise(False))


conditions = [
    ((col("post_type") == "Order") | (col("post_type") == "Refund"), "mysql2snowflake")
]

s_df = qualify_rn_df.withColumn("source_system", when(conditions[0][0], conditions[0][1]).otherwise("NA"))

s_df.write.jdbc(url=postgres_url, table="genaidb.sales_view", mode="append", properties=postgres_properties)


spark.stop()
