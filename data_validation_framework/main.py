import argparse
import datetime
import json
import logging
import os
import sys
import pandas as pd


from utils.sql_server_connector import (
    db_command_executer_with_output
)

file_as_of_date = datetime.datetime.now().strftime("%Y-%m-%d")

logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(
    __name__,
)


def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--JSON_FILE_PATH")
    parser.add_argument("--LOG_PATH")

    parser.add_argument("--MYSQL_USERNAME1")
    parser.add_argument("--MYSQL_PASSWORD1")
    parser.add_argument("--MYSQL_HOST1")
    parser.add_argument("--MYSQL_PORT1")

    parser.add_argument("--POSTGRESQL_USERNAME1")
    parser.add_argument("--POSTGRESQL_PASSWORD1")
    parser.add_argument("--POSTGRESQL_HOST1")
    parser.add_argument("--POSTGRESQL_PORT1")

    parsed_args = parser.parse_args()
    parsed_args = vars(parsed_args)
    
    if parsed_args.get("JSON_FILE_PATH") is not None:
        with open(parsed_args.JSON_FILE_PATH, "r") as fh:
            validation_dict = json.load(fh)
    else:
        with open("dataflow.json", "r") as fh:
            validation_dict = json.load(fh)

    if parsed_args.get("LOG_PATH") is None:
        parsed_args["LOG_PATH"] = validation_dict["env"].get("LOG_PATH")

    logging_file_name = "_".join(
        ["data_validation", str(file_as_of_date)]
    )
    logging_full_pathname = os.path.join(os.getcwd(), logging_file_name)
    if parsed_args["LOG_PATH"] is not None:
        file_log_path = os.path.expanduser(parsed_args["LOG_PATH"])
        logging_full_pathname = os.path.join(file_log_path, logging_file_name)

    if os.path.exists(logging_full_pathname):
        os.remove(logging_full_pathname)
    fh = logging.FileHandler(logging_full_pathname)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.info("Starting run at " + str(datetime.datetime.now()))
    logger.info("Logging to " + os.path.join(file_log_path, logging_file_name))
    return parsed_args, validation_dict


def validate_data(validation_dict):
    logger.info(f"Execution started")
    all_tests = list(validation_dict.keys())
    results = []
    for index, each_test in enumerate(all_tests):
        logger.info(f"\nValidation of test case {index+1} started")
        type_of_test = list(validation_dict[each_test].keys())[0]
        if type_of_test=="count_validation":
            source_query = f"SELECT COUNT(*) FROM {validation_dict[each_test][type_of_test]['source']['db']}.{validation_dict[each_test][type_of_test]['source']['table']}"
            destination_query = f"SELECT COUNT(*) FROM {validation_dict[each_test][type_of_test]['target']['db']}.{validation_dict[each_test][type_of_test]['target']['table']}"
            source_count = db_command_executer_with_output(validation_dict[each_test][type_of_test]['source']['server'], validation_dict[each_test][type_of_test]['source']['db'], source_query).iloc[0,0]
            destination_count = db_command_executer_with_output(validation_dict[each_test][type_of_test]['target']['server'], validation_dict[each_test][type_of_test]['target']['db'], destination_query).iloc[0,0]
            if (source_count==destination_count): run_status="Success"
            else: run_status="Failed"
            results.append((index+1, run_status, f"Source count: {source_count}, destination count: {destination_count}"))
        
        if type_of_test=="duplicate_check":
            query = f"SELECT {validation_dict[each_test][type_of_test]['columns_to_check']} FROM {validation_dict[each_test][type_of_test]['db']}.{validation_dict[each_test][type_of_test]['table']} GROUP BY {validation_dict[each_test][type_of_test]['columns_to_check']} HAVING COUNT(*)>1"
            duplicates = db_command_executer_with_output(validation_dict[each_test][type_of_test]['server'], validation_dict[each_test][type_of_test]['db'], query)
            if duplicates.shape[0]==0: 
                run_status="Success"
                results.append((index+1, run_status, f"Duplicates doesn't exist"))
            else: 
                run_status="Failed"
                results.append((index+1, run_status, f"Duplicates exist"))
            
        if type_of_test=="is_null_check":
            columns_to_check = validation_dict[each_test][type_of_test]['columns_to_check']
            condition_string = "WHERE " + " IS NULL OR ".join(columns_to_check.split(",")) + " IS NULL LIMIT 10"
            query = f"SELECT * FROM {validation_dict[each_test][type_of_test]['db']}.{validation_dict[each_test][type_of_test]['table']} " + condition_string
            nulls = db_command_executer_with_output(validation_dict[each_test][type_of_test]['server'], validation_dict[each_test][type_of_test]['db'], query)
            if nulls.shape[0]==0:
                run_status="Success"
                results.append((index+1, run_status, f"No nulls"))
            else:
                run_status="Failed"
                results.append((index+1, run_status, f"Nulls exist"))
    return pd.DataFrame(results, columns=["Test case", "Testing status", "Remarks"])

    
def html_table_creator(df):
    df = df.reset_index(drop=True)
    df = df.iloc[:,1:]
    html = """<h2> Validation status </h2>
        <table border='1'>
        <tr>
            <th style='background-color:rgb(0, 153, 255);'>Test case No</th>
            <th style='background-color:rgb(0, 153, 255);'>Testing status</th>
            <th style='background-color:rgb(0, 153, 255);'>Remarks</th>
        </tr>"""
    
    for index, row in df.iterrows():
        if str(row["Testing status"]).lower() == "success":
            html = html + "<tr>"
            html = html + "<td>" + str(index + 1) + "</td>"
            for col in row:
                if str(col).lower() == "success":
                    html = (
                        html
                        + "<td style='background-color:rgb(102, 255, 153);'>"
                        + str(col)
                        + "</td>"
                    )
                else:
                    html = html + """<td>""" + str(col) + "</td>"
            html = html + "</tr>"
        else:
            html = html + "<tr>"
            html = html + "<td>" + str(index + 1) + "</td>"
            for col in row:
                if str(col).lower() == "failed":
                    html = (
                        html
                        + """<td style='background-color:rgb(255, 0, 0);'>"""
                        + str(col)
                        + "</td>"
                    )
                else:
                    html = html + """<td>""" + str(col) + "</td>"
            html = html + "</tr>"
    return html


if __name__ == "__main__":
    args, validation_dict = argument_parser()
    logger.info("\n")
    try:
        df_results = validate_data(
            validation_dict=validation_dict["data_validation"]
        )
        result_html = html_table_creator(df_results)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        with open(f'/opt/airflow/plugins/templates/test_results/validation_results_{timestamp}.html', 'w') as file:
            file.write(result_html)
        logger.info("Execution completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.info("Failed due to error")
        logger.info(e)
        print("Failed due to error: ")
        print(e)
        sys.exit(-1)
