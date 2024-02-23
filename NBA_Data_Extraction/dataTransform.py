from s3_bucket import s3client, uploadFile
import awswrangler as wr
import boto3
from pyspark.sql import SparkSession
from config import makeCSV
from db_connection import createEngine
from pyspark.sql import SparkSession
import pandas as pd
from io import StringIO


def transformData(appName, bucket_name, file_key):

    #Read csv file from S3 on aws
    try:
        csv_obj = s3client.get_object(Bucket=bucket_name, Key=file_key)
    except(Exception) as error:
        print(f'Could Not read csv using s3client , {error}')
        exit()
    
    #Columns from unstructred data we want to keep (Change if needed)
    col_to_keep = 'RANK,PLAYER,TEAM,FGM,FG3M,FTM,PTS,EFF'
    col_to_keep = col_to_keep.split(',')

    #Create a dataFrame based on the csv from S3
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df_pandas = pd.read_csv(StringIO(csv_string))

    #Use spark to transform the CSV files
    try:
        spark = SparkSession.builder.appName(appName).getOrCreate()
        df_spark = spark.createDataFrame(df_pandas)
        df_selected = df_spark.select(*col_to_keep)
    except(Exception) as error:
        print(f'Could not create sparkSession, {error}')

    backToPd_Df = df_selected.toPandas()

    silver_folder_name = 'silver_nba_stats_csv'
    type_of_data = 'silver'
    year_range = '2020-21'
    season_type = 'Regular'

    whole_file_path, output_csv_name = makeCSV(backToPd_Df, silver_folder_name, year_range, season_type, type_of_data)
    print(output_csv_name)
    try:
        uploadFile(bucket_name, whole_file_path, output_csv_name)
        print(f'Successfully Uploaded transformed (Silver) data to S3 Bucket: {bucket_name}') 
        print('Successfully created a new table in the database based off of this silver data!')
        createEngine(bucket_name, output_csv_name, appName)
    except:
	    print("Could not upload transformed (Silver) data file to AWS S3 Bucket")