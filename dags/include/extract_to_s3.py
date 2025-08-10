from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
import boto3
import pyarrow as pa, pyarrow.parquet as pq
import requests

load_dotenv()

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET             = os.getenv("S3_BUCKET")
API_URL               = "https://jsonplaceholder.typicode.com/posts"

def fetch_api() -> pd.DataFrame:
    print("Requesting data ...")
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    print(f"Fetched {len(df)} records.")
    return df

def to_parquet(df: pd.DataFrame) -> str:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    filename = f"api_snapshot_{ts}.parquet"
    print(f"Writing {filename} ...")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename, compression="snappy")
    return filename

def upload_file(local_file: str, bucket: str, key: str):
    if not bucket:
        raise RuntimeError("S3_BUCKET not set in .env")
    print(f"Uploading to s3://{bucket}/{key} ...")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    s3.upload_file(local_file, bucket, key)
    print("Upload complete ✅")

if __name__ == "__main__":
    df = fetch_api()
    local_parquet = to_parquet(df)
    s3_key = f"raw/{local_parquet}"
    upload_file(local_parquet, S3_BUCKET, s3_key)
