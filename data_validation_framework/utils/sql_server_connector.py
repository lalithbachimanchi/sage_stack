import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


def source_database_connect(database):
    load_dotenv()
    username = os.environ.get("MYSQL_USER")
    password = os.environ.get("MYSQL_PASSWORD")
    host = os.environ.get("MYSQL_HOST_NAME")
    port = os.environ.get("MYSQL_PORT")       
    
    # MySQL connection string
    _conn_string = f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}'
    _engine = create_engine(_conn_string)
    return _engine


def destination_database_connect(database):
    load_dotenv()
    username = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("POSTGRES_HOST_NAME")
    port = os.environ.get("POSTGRES_PORT")  

    # MySQL connection string
    _conn_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
    _engine = create_engine(_conn_string)
    return _engine


def db_command_executer_with_output(server, database, cmd, full_data=False):
    if server=='mysql':
        _engine = source_database_connect(database)
    else:
        _engine = destination_database_connect(database)

    connection = _engine.raw_connection()
    try:
        cursor_obj = connection.cursor()
        print(f"Executing command: {cmd}")
        print(f"Db: {database}")
        cursor_obj.execute(cmd)
        if not full_data:
            result = cursor_obj.fetchmany(10)
        else:
            result = cursor_obj.fetchall()
        field_names = [i[0] for i in cursor_obj.description]
        cursor_obj.close()
    finally:
        connection.close()
    df = pd.DataFrame([list(i) for i in result], columns=field_names)
    return df
