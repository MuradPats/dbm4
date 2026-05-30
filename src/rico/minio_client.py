"""MinIO / S3 client helpers."""
import os

import boto3
from botocore.exceptions import ClientError

MINIO_URL = os.environ["MINIO_URL"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]


def get_s3():
    """Return a boto3 S3 client pointed at MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def object_exists(s3, key: str) -> bool:
    """Return True if the object already exists in the bucket."""
    try:
        s3.head_object(Bucket=MINIO_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def put_object(s3, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    """PUT bytes to MinIO. Idempotent — same key overwrites the same bytes."""
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=data, ContentType=content_type)


def get_object(s3, key: str) -> bytes:
    """GET bytes from MinIO."""
    return s3.get_object(Bucket=MINIO_BUCKET, Key=key)["Body"].read()
