import boto3
import pandas as pd
from io import StringIO


def load_data_from_s3(bucket, file_name):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=file_name)
    data = obj["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(data))


data = load_data_from_s3("your_bucket_name", "your_file.csv")
