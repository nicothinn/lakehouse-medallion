"""Sube data/*.parquet a Bronze (overwrite). Ya ejecutado; rerunnable."""
import os
import boto3

EP = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
s3 = boto3.client("s3", endpoint_url=EP,
                  aws_access_key_id=os.getenv("MINIO_USER", "admin"),
                  aws_secret_access_key=os.getenv("MINIO_PASSWORD", "password"),
                  region_name="us-east-1")
ex = {b["Name"] for b in s3.list_buckets()["Buckets"]}
for bucket in ("lakehouse",):
    if bucket not in ex:
        s3.create_bucket(Bucket=bucket)
        print("bucket creado:", bucket)
jobs = [("data/post_2020.parquet", "bronze/post/2020/post_2020.parquet"),
        ("data/post_2021.parquet", "bronze/post/2021/post_2021.parquet"),
        ("data/users_2020.parquet", "bronze/users/2020/users_2020.parquet"),
        ("data/users_2021.parquet", "bronze/users/2021/users_2021.parquet")]
for local, remote in jobs:
    s3.upload_file(local, "lakehouse", remote)  # PUT = overwrite
    print(f"OK {remote} ({os.path.getsize(local) // 1024} KB)")
print("BRONZE UPLOAD DONE")
