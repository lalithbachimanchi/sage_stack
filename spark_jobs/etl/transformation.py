import os
import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

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

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL Schema to Empty DataFrame Schema") \
    .getOrCreate()

# Define the schema for the sales table
sales_schema = StructType([
    # Columns from the users table
    StructField("user_id", IntegerType(), True),
    StructField("user_login", StringType(), True),
    StructField("user_pass", StringType(), True),
    StructField("user_nicename", StringType(), True),
    StructField("user_email", StringType(), True),
    StructField("user_url", StringType(), True),
    StructField("user_registered", TimestampType(), True),
    StructField("user_activation_key", StringType(), True),
    StructField("user_status", IntegerType(), True),
    StructField("display_name", StringType(), True),

    # Columns from the usermeta table
    StructField("usermeta_id", IntegerType(), True),
    StructField("user_meta_key", StringType(), True),
    StructField("user_meta_value", StringType(), True),

    # Columns from the posts table
    StructField("post_id", IntegerType(), True),
    StructField("post_author", IntegerType(), True),
    StructField("post_date", TimestampType(), True),
    StructField("post_date_gmt", TimestampType(), True),
    StructField("post_content", StringType(), True),
    StructField("post_title", StringType(), True),
    StructField("post_excerpt", StringType(), True),
    StructField("post_status", StringType(), True),
    StructField("comment_status", StringType(), True),
    StructField("ping_status", StringType(), True),
    StructField("post_password", StringType(), True),
    StructField("post_name", StringType(), True),
    StructField("to_ping", StringType(), True),
    StructField("pinged", StringType(), True),
    StructField("post_modified", TimestampType(), True),
    StructField("post_modified_gmt", TimestampType(), True),
    StructField("post_content_filtered", StringType(), True),
    StructField("post_parent", IntegerType(), True),
    StructField("guid", StringType(), True),
    StructField("menu_order", IntegerType(), True),
    StructField("post_type", StringType(), True),
    StructField("post_mime_type", StringType(), True),
    StructField("comment_count", IntegerType(), True),

    # Columns from the postmeta table
    StructField("post_meta_id", IntegerType(), True),
    StructField("post_meta_key", StringType(), True),
    StructField("post_meta_value", StringType(), True),

    # Columns from the commerce_order_items table
    StructField("order_item_id", IntegerType(), True),
    StructField("order_item_name", StringType(), True),
    StructField("order_item_type", StringType(), True),
    StructField("order_id", IntegerType(), True),

    # Columns from the commerce_order_itemsmeta table
    StructField("order_meta_id", IntegerType(), True),
    StructField("order_meta_key", StringType(), True),
    StructField("order_meta_value", StringType(), True)
])

# Create an empty DataFrame with the specified schema
empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=sales_schema)

# Show the empty DataFrame schema
empty_df.printSchema()

# Stop the SparkSession
spark.stop()
