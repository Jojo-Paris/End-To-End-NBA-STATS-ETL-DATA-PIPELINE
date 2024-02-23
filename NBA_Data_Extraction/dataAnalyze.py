from pyspark.sql import SparkSession, SQLContext
from s3_bucket import bucket_name, uploadFile
from config import read_s3, makeCSV
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2 import Error
from s3_bucket import uploadFile

table_name = 'gold_data'
year = '2020-21'
season_type = 'Regular'

def anaylze(appName, db_name_, df, dtype_Map):

    db_endpoint = "postgres-1.c3oi8gmo2gpy.us-east-2.rds.amazonaws.com"
    db_port = "5432"
    db_user = 'postgres'
    db_password = XXXX

    #Use spark to transform the CSV files
    try:
        spark = SparkSession.builder.appName(appName).getOrCreate()
        df_spark = spark.createDataFrame(df)
    except(Exception) as error:
        print(f'Could not create sparkSession, {error}')

    df_spark.createOrReplaceTempView("my_temp_table")

    query = """
            SELECT
                TEAM,
                SUM(PTS) AS TOTAL_POINTS,
                SUM(FGM) AS TOTAL_FGM,
                SUM(FG3M) AS TOTAL_FG3M
            FROM
                my_temp_table
            GROUP BY
                TEAM
            ORDER BY
                TOTAL_POINTS DESC
        """
    
    result = spark.sql(query)
    result.show()
    print(result.count())
    df = result.toPandas()

    print(df)

    connection = psycopg2.connect(
                host=db_endpoint,
                user=db_user,
                password=db_password,
                database=db_name_
    )

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    try:
        column_types = df.dtypes
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')

        sql_script = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        sql_script += "    ID SERIAL,\n"
        for column_name, data_type in column_types.items():
            sql_data_type = dtype_Map.get(str(data_type))
            sql_script += f"    {column_name} {sql_data_type},\n"

        sql_script = sql_script.rstrip(',\n') + "\n);"
        print(sql_script)
        cursor.execute(sql_script)
        print('sucessfully created sql script for gold data and inserted into database')
    except Exception as e:
        print('could not drop gold data table or create gold data table, ', e)
        

    try:
        for row in df.itertuples(index=False):
            columns = [f'{col}' for col in df.columns]
            values = [
                row.TEAM.strip(),
                int(row.TOTAL_POINTS),
                int(row.TOTAL_FGM),
                int(row.TOTAL_FG3M),
            ]

            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(values))})"
            # Execute the query with parameterized values
            cursor.execute(insert_query, values)
        connection.commit   
        print("succesfully added gold dataframe to the sql table")
    except Exception as e:
        print('Could not add dataframe for gold data into the sql table,' , e)
        exit()
    finally:
        cursor.close()
        connection.close()
    output_folder_name = f'gold_nba_stats_csv'
    type_of_data = 'gold'
    try:
        whole_file_path, output_csv_name = makeCSV(df, output_folder_name, year, season_type, type_of_data)
        uploadFile(bucket_name, whole_file_path, output_csv_name)
        print('successfully created csv and uploaded files to s3 for gold data!')
    except Exception as e:
        print("Could not make csv for gold data or upload file for gold data, ", e)
    

'''
df = read_s3(bucket_name, 'silver_nba_stats_2019-20_Regular.csv')
dtype_Map = {
            'int64':'INT',
            'object' : 'VARCHAR(255) NOT NULL',
            'float64' : 'FLOAT',
            'id' : 'SERIAL'
    }

anaylze('NBADataTransformation','nba_stats_2019_20_Regular_db', df, dtype_Map)'''

