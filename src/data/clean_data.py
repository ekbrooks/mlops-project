import pandas as pd
import os
import boto3
from dotenv import load_dotenv

# load environment variables
session = boto3.Session(profile_name="mlops-user")
sts = session.client("sts")
identity = sts.get_caller_identity()

print(identity)

# load environment
load_dotenv()

# aws credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# intialize s3 client
s3_client = boto3.client(
    "s3",
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION
)