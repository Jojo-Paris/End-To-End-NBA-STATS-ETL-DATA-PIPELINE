from configparser import ConfigParser
import os
import pandas as pd
import s3fs

def config(filename='db.ini', section='postgres'):
    parser = ConfigParser()
    parser.read(filename)
    db={}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section{section} is not found in the {filename} file')
    
    return db

def makeCSV(df, output_folder_name, year_range, season_type, type_of_data):
	output_folder = r'C:\Users\jojon\OneDrive\Desktop\Data Engineering Projects\NBA_Data_Extraction\{0}'.format(output_folder_name)
	output_csv_name = f'{type_of_data}_nba_stats_{year_range}_{season_type}.csv'
	os.makedirs(output_folder, exist_ok=True)
	csv_file_path = os.path.join(output_folder, output_csv_name)

	#Save the dataframe to the specified folder and file
	if isinstance(df, pd.core.frame.DataFrame):
		df.to_csv(csv_file_path, index=False)

	return [output_folder + '\{0}'.format(output_csv_name), output_csv_name]

def read_s3(bucket_name, output_file):
    s3_key = XXXX
    s3_secret = XXXX

    try:
        # Create S3 filesystem object
        s3 = s3fs.S3FileSystem(anon=False, key=s3_key, secret=s3_secret)

        # Read the CSV file from S3 into a Pandas DataFrame
        with s3.open(f'{bucket_name}/{output_file}', 'rb') as file:
            df = pd.read_csv(file)
        print('Successfully created a new dataframe based on the given S3 bucket')
        
    except Exception as e:
        print(e)
        exit()
    
    return df