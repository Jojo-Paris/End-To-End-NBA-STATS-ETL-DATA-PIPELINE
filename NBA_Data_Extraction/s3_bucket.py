import boto3
from botocore.exceptions import NoCredentialsError

sess = boto3.Session(region_name='us-west-1', aws_access_key_id= XXXX, aws_secret_access_key= XXXX)
s3client = sess.client('s3')

bucket_name = 'jodeciparis-s3-nba-data'
s3_location = {
    'LocationConstraint': 'us-west-1'
}

def createBucket(bucket_name, s3_location):
    s3client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=s3_location)

def uploadFile(bucket_name, csv_file_path, csv_file_name):
    s3client.upload_file(csv_file_path, bucket_name, csv_file_name)