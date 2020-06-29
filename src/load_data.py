'''Upload data to Redshift via S3'''
import re
from io import StringIO
import configparser
import boto3
import psycopg2
from psycopg2 import sql
from sqlite3 import OperationalError, ProgrammingError
import pandas as pd

def connect_database():
    config = configparser.ConfigParser()
    config.read("./reference/config.ini")
    
    con = psycopg2.connect(dbname=config["db_info"]["db"],
                           host=config["db_info"]["host"],
                           port=config["db_info"]["port"],
                           user=config["db_info"]["user"],
                           password=config["db_info"]["pass"])
    print("Connected to database.")
    return con

def connect_s3():
    config = configparser.ConfigParser()
    config.read("./reference/config.ini")
    
    s3 = boto3.resource('s3',
                        region_name=config["s3_info"]["region_name"],
                        aws_access_key_id=config["s3_info"]["key_id"],
                        aws_secret_access_key=config["s3_info"]["secret_key"])
    return s3

def load_csv(data):
    s3 = connect_s3()
    
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, header=False, index=False)
    s3.Object(config["s3_info"]["bucket_name"],
              "data_file.csv").put(Body=csv_buffer.getvalue())
    print("Upload to S3 complete.")
    
    exec_sql_file(cursor, './reference/sql_script.sql')
    s3.Object(config["s3_info"]["bucket_name"],
              "data_file.csv").delete()
    print("Upload to database complete.")
    
def exec_sql_file(cursor, sql_file):
    config = configparser.ConfigParser()
    config.read("./reference/config.ini")
    
    print("\nExecuting SQL script file: '%s'" % (sql_file))
    statement = ""

    for line in open(sql_file):
        if re.match(r'--', line):  
            continue
        if not re.search(r';$', line):  
            statement = statement + line
        else:  
            statement = statement + line
            if "COPY" in statement:
                statment = statement.format(
                    key_id=config["s3_info"]["key_id"],
                    secret_key=config["s3_info"]["secret_key"])
            try:
                cursor.execute(statement)
            except (OperationalError, ProgrammingError) as e:
                print("\n[WARN] MySQLError during execute statement \n\tArgs: '%s'" % (str(e.args)))

                        
                         