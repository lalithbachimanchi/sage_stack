from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import col, current_date, datediff, year
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import expr
from pyspark.sql.functions import when, length, regexp_replace



# Define a UDF (User-Defined Function) to format phone numbers
def format_phone_number(phone_number):
    return when(length(phone_number) == 10, regexp_replace(phone_number, r'(\d{3})(\d{3})(\d{4})', r'($1) $2-$3')).otherwise(phone_number)


# Initialize a Spark session
spark = SparkSession.builder \
    .appName("CSV to MySQL DB") \
    .config("spark.driver.extraClassPath", "mysql-connector-j-8.1.0.jar") \
    .getOrCreate()

# Read the CSV file into a DataFrame
csv_file_path = "/root/data/sample_etl_data.csv"  # Replace with your CSV file path
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df.show()


# Calculate age based on DOB
df_with_age = df.withColumn("age_in_years",
    year(current_date()) - year(col("date_of_birth")) -
    ((datediff(current_date(), col("date_of_birth")) < 0).cast("int")))

df_with_age.show()


# Apply the UDF to format phone numbers
df_formatted = df.withColumn("formatted_phone_number", format_phone_number(col("phone_number")))

# Show the DataFrame with formatted phone numbers
df_formatted.show()


# Define the JDBC URL
jdbc_url = "jdbc:mysql://mysql_container:3306/genaidb"

# Define the connection properties
connection_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Define the table name where you want to upload the data
table_name = "health_care_data"

# Write the DataFrame to MySQL
df_formatted.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

# Stop the Spark session
spark.stop()


