import pandas as pd
import os
import boto3
from dotenv import load_dotenv
from io import StringIO

# load environment
load_dotenv()

# load environment variables
session = boto3.Session(profile_name="mlops-user")
sts = session.client("sts")
identity = sts.get_caller_identity()

print(identity)

# aws credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# intialize s3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name = AWS_REGION
)

# grabbing data from s3 and putting into dataframe
def fetch_data_from_s3(file_key):
    try:
        obj = s3_client.get_object(Bucket = AWS_BUCKET_NAME, Key = file_key)
        df = pd.read_csv(obj['Body'])
        print(f"Successfully fetched data {file_key} from s3.")
        return df
    except Exception as e:
        print(f"Error fetching data from s3: {e}")


# saving cleaning/processed data back to s3
def save_data_to_s3(df, file_key):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index = False)
        s3_client.put_object(Bucket = AWS_BUCKET_NAME, Key = file_key, Body = csv_buffer.getvalue())
        print(f"Successfully saved {file_key} to s3")
    except Exception as e:
        print(f"Error saving data to s3: {e}")
