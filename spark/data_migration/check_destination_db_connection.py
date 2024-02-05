import os
import json

from pyspark.sql import SparkSession

MYSQL_CONNECTOR_PATH = os.environ['MYSQL_CONNECTOR_PATH']
destination_db_host=os.environ['POSTGRES_HOST_NAME']
destination_db_database=os.environ['POSTGRES_DATABASE']
destination_db_port=os.environ['POSTGRES_PORT']
destination_db_user_name=os.environ['POSTGRES_USER']
destination_db_password=os.environ['POSTGRES_PASSWORD']

# Set up Spark session
spark = SparkSession.builder\
    .appName("PostgresDBConnection")\
    .getOrCreate()

# PostgreSQL connection parameters
postgres_url = f"jdbc:postgresql://{destination_db_host}:{destination_db_port}/{destination_db_database}"
postgres_properties = {
    "user": destination_db_user_name,
    "password": destination_db_password,
    "driver": "org.postgresql.Driver",
}

with open("TABLE_MAPPING_JSON_PATH", "r") as table_mapping_file:
    table_mapping_dict = json.loads(table_mapping_file.read())

# just the first table on destination database
table_name = table_mapping_dict.values()[0]
df = spark.read.jdbc(url=postgres_url, table=table_name, properties=postgres_properties)

df.show()

spark.stop()