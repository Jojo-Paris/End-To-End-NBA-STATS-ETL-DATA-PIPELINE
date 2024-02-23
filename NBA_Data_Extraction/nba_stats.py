#https://www.nba.com/stats/players/traditional?sort=PTS&dir=-1&Season=2020-21
import pyspark
import os
import requests
import pandas as pd
import time
import numpy as np
from s3_bucket import uploadFile, bucket_name
import csv
from config import makeCSV

#Start Year-End Year (XXXX)-(XX)
year_range = '2020-21'

#Regular, Playoffs
season_type = 'Regular'

#What to sort by (PTS, MIN, FGM), WIll get rid of some stats later on
stat_category = 'PTS'

#Request the JSON URL from the NBA Stats website 
nba_url = f'https://stats.nba.com/stats/leagueLeaders?LeagueID=00&PerMode=Totals&Scope=S&Season={year_range}&SeasonType={season_type}%20Season&StatCategory={stat_category}'
r = requests.get(url=nba_url).json()

#Grab the headers (PTS, FGM, Team, etc)
table_headers = r['resultSet']['headers']

#Grab the individual 
individual_player = r['resultSet']['rowSet']

#Create the main dataFrame of all of our players
player_df = pd.DataFrame(individual_player, columns=table_headers) 

raw_folder_name = 'raw_nba_stats_csv'
type_of_data = 'raw'
whole_file_path, output_csv_name = makeCSV(player_df, raw_folder_name,year_range, season_type, type_of_data)

# Will upload csv file into AWS S3 bucket (Uncomment when we want to put into the bucket)
try:
	uploadFile(bucket_name, whole_file_path, output_csv_name)
	print(f'Successfully Uploaded to S3 Bucket: {bucket_name}')
except:
	print("Could not upload raw data file to AWS S3 Bucket")

from dataTransform import transformData

appName = 'NBADataTransformation'
transformData(appName, bucket_name, output_csv_name)

print("The code has been completed, this is what we have accomplished:"
	  "- We have successfully created a csv file for raw data, silver data and gold data."
	  "- Each of these have been inserted into the personal Amazon S3 bucket, for future use."
	  "- The silver data and gold data have been saved into my Amazon RDS Postgres Database"
	  "- Currently going to work on data visualization using python libraries"
	  "- Currently going to work on automation the workflow using Apace Airflow"
	  "- Raw Data = All NBA stats for a specific season year"
	  "- Silver Data = Transformed raw data using Pyspark and Pandas to get only stats related points so it can be used for future use (Automated)"
	  "- Gold Data = Transformed the Silver data to show the totals for categories based on team")