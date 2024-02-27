import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

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

query = """SELECT DISTINCT coi.order_item_id, coi.order_item_name,
 coi.order_item_type, coi.order_id, p.post_id, p.post_content_filtered, p.post_type, p.post_name,
 p.post_title, p.post_date, p.post_excerpt, p.post_status,
  u.user_id, u.user_email, u.user_url, u.user_status, u.display_name
FROM genaidb.commerce_order_items coi
JOIN genaidb.posts p ON coi.order_user = p.post_author
JOIN genaidb.users u ON coi.order_user = u.user_id
AND NOT EXISTS (
    SELECT 1
    FROM genaidb.sales_view s
    WHERE s.user_id = u.user_id
)
"""

final_df = spark.read.format("jdbc") \
    .option("url", postgres_url) \
    .option("query", query) \
    .options(**postgres_properties) \
    .load()

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
