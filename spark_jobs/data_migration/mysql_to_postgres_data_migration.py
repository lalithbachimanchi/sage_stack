import os
import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
    .appName("MySQLToPostgresMigration").getOrCreate()

# MySQL connection parameters
mysql_url = f"jdbc:mysql://{source_db_host}:{source_db_port}/{source_db_database}"
mysql_properties = {
    "user": source_db_user_name,
    "password": source_db_password,
    "driver": "com.mysql.cj.jdbc.Driver",
}

# PostgreSQL connection parameters
postgres_url = f"jdbc:postgresql://{destination_db_host}:{destination_db_port}/{destination_db_database}"
postgres_properties = {
    "user": destination_db_user_name,
    "password": destination_db_password,
    "driver": "org.postgresql.Driver",
}

with open(os.environ["TABLE_MAPPING_JSON_PATH"], "r") as table_mapping_file:
    table_mapping_dict = json.loads(table_mapping_file.read())

for source_table, destination_table in table_mapping_dict.items():
    print(f"Processing tables: {source_table} to {destination_table}")
    # Read data from MySQL
    mysql_df = spark.read.jdbc(url=mysql_url, table=source_table, properties=mysql_properties)

    print(f"Number of rows on source table before migration: {mysql_df.count()}")

    # Calculate the timestamp for one hour ago
    one_min_ago = datetime.now() - timedelta(minutes=5)

    # Filter data based on the timestamp condition
    # filtered_mysql_df = mysql_df.filter(timestamp_condition)
    filtered_mysql_df = mysql_df.filter((col("created") >= one_min_ago) & (col("created") < datetime.now()))
    # filtered_mysql_df = mysql_df.filter(col("created") < one_min_ago)

    print(f"Number of rows from source being inserted into destination: {filtered_mysql_df.count()}")

    destination_df = spark.read.jdbc(url=postgres_url, table=destination_table, properties=postgres_properties)
    print(f"Number of rows on destination table before migration: {destination_df.count()}")

    # Write data to PostgreSQL
    filtered_mysql_df.write.jdbc(url=postgres_url, table=destination_table, mode="append", properties=postgres_properties)

    destination_df = spark.read.jdbc(url=postgres_url, table=destination_table, properties=postgres_properties)

    print(f"Number of rows on destination table after migration: {destination_df.count()}")

# Stop Spark session
spark.stop()
