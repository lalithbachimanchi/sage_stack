import os
import json

from pyspark.sql import SparkSession

source_db_host=os.environ['MYSQL_HOST_NAME']
source_db_database=os.environ['MYSQL_DATABASE']
source_db_port=os.environ['MYSQL_PORT']
source_db_user_name=os.environ['MYSQL_USER']
source_db_password=os.environ['MYSQL_PASSWORD']

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

with open(os.environ['TABLE_MAPPING_JSON_PATH'], "r") as table_mapping_file:
    table_mapping_dict = json.loads(table_mapping_file.read())

# just the first table on source database
table_name = list(table_mapping_dict.keys())[0]
df = spark.read.jdbc(url=mysql_url, table=table_name, properties=mysql_properties)

df.show()

assert df.count() != 0, "There are no rows in the source Database"

spark.stop()