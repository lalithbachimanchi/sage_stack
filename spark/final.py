from pyspark_ai.pyspark_ai import SparkAI
import os
import argparse
import pandas as pd


def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--CSV_DATA_FILE_PATH")
    parser.add_argument("--DF")
    parser.add_argument("--REQUIREMENT")
    parsed_args = parser.parse_args()
    return parsed_args


def generate_sql_query(requirement, csv_data_file_path=None, df=None):
    if csv_data_file_path is None and df is None:
        return 0
    elif csv_data_file_path is not None:
        df = pd.read_csv(csv_data_file_path)

    spark_ai = SparkAI()
    spark_ai.activate()
    df_spark = spark_ai._spark.createDataFrame(df)
    _, required_sql_query = df_spark.ai.transform(requirement)
    return required_sql_query


if __name__=="__main__":
    args = argument_parser()
    requirement = args["REQUIREMENT"]
    df = args["DF"]
    csv_data_file_path = args["CSV_DATA_FILE_PATH"]
    os.environ["OPENAI_API_KEY"] = ""
    query = generate_sql_query(requirement, csv_data_file_path, df)
    print(query)
    
