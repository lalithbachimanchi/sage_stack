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
    .appName("Transformation").getOrCreate()

# PostgreSQL connection parameters
postgres_url = f"jdbc:postgresql://{destination_db_host}:{destination_db_port}/{destination_db_database}"
postgres_properties = {
    "user": destination_db_user_name,
    "password": destination_db_password,
    "driver": "org.postgresql.Driver",
}


