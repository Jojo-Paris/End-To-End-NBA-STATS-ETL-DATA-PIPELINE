import psycopg2
from psycopg2 import Error
from config import config, read_s3
import s3fs
from io import StringIO, BytesIO
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
import os
from s3_bucket import bucket_name
import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import dataAnalyze

db_endpoint = "postgres-1.c3oi8gmo2gpy.us-east-2.rds.amazonaws.com"
db_port = "5432"
db_name = "postgres"
db_user = 'postgres'
db_password = XXXX
year = '2020-21'
season_type = 'Regular'
table_name = 'silver_data'

def connect_to_db(output_file):
    connection = None
    params = config()
    print("Connecting to the postgreSQL Database ...")
    connection = psycopg2.connect(**params)
    cursor = connection.cursor()

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    db_name_ = f'nba_stats_{year}_{season_type}'
    db_name_ = db_name_ + '_db'
    db_name_ = db_name_.replace("-", "_")

    try:
        cursor.execute('DROP DATABASE IF EXISTS "{}" WITH (FORCE)'.format(db_name_))
        cursor.execute('CREATE DATABASE "{}"'.format(db_name_))
        connection.commit()
        connection.close()
        print(f'Successfuly created the new Database {db_name_}')
    except Exception as e:
        print('Could not drop or create the database, ', e)
    
    return db_name_

def createTable(db_name_, df, column_types, dtype_Map):
    connection = psycopg2.connect(
                host=db_endpoint,
                user=db_user,
                password=db_password,
                database=db_name_
    )

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    try:

        
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')

        sql_script = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        sql_script += "    ID SERIAL,\n"
        for column_name, data_type in column_types.items():
            sql_data_type = dtype_Map.get(str(data_type))
            sql_script += f"    {column_name} {sql_data_type},\n"

        sql_script = sql_script.rstrip(',\n') + "\n);"
        print(sql_script)
        
        cursor.execute(sql_script)
        print('sucessfully created sql script and inserted into database')
    except Exception as e:
        print('could not drop table or create table, ', e)

    try:
        for row in df.itertuples(index=False):
            columns = [f'{col}' for col in df.columns]
            values = [
                int(row.RANK),
                row.PLAYER.strip(),  # Remove leading/trailing spaces
                row.TEAM.strip(),    # Remove leading/trailing spaces
                int(row.FGM),
                int(row.FG3M),
                int(row.FTM),
                int(row.PTS),
                int(row.EFF)
            ]
            
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(values))})"
            # Execute the query with parameterized values
            cursor.execute(insert_query, values)

        connection.commit   

        print("succesfully added dataframe to the sql table")
    except Exception as e:
        print('Could not add dataframe data into the sql table,' , e)
        exit()
    finally:
        cursor.close()
        connection.close()

def createEngine(bucket_name, output_file, appName):

    df = read_s3(bucket_name, output_file)
    column_types = df.dtypes
    dtype_Map = {
            'int64':'INT',
            'object' : 'VARCHAR(255) NOT NULL',
            'float64' : 'FLOAT',
            'id' : 'SERIAL'
    }

    db_name_ = connect_to_db(output_file)
    createTable(db_name_, df, column_types, dtype_Map)
    dataAnalyze.anaylze(appName, db_name_, df, dtype_Map)

#createEngine(bucket_name, 'silver_nba_stats_2014-15_Regular.csv')