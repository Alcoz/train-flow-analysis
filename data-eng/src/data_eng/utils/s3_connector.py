"""Interface with boto3 for s3 management."""

import boto3


def connect_to_s3(
    endpoint_url: str,
    access_key_id: str,
    secret_access_key: str,
    region_name: str,
):
    """Create an S3 client from the given credentials.

    Args:
        endpoint_url (str): URL of the S3 endpoint (useful for S3-compatible
            services like MinIO, or for a specific AWS endpoint).
        access_key_id (str): AWS Access Key ID.
        secret_access_key (str): AWS Secret Access Key associated with the access key.
        region_name (str): Name of the S3 region (e.g. "eu-west-3").

    Returns:
        boto3.client : The created S3 client

    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region_name,
    )


def get_object_from_s3(
    s3_client,
    bucket: str,
    filepath: str,
):
    """Retrieve the content of an object stored on S3.

    Args:
        s3_client (boto3.client): An already initialized S3 client (see `connect_to_s3`).
        bucket (str): Name of the S3 bucket containing the object.
        filepath (str): Path (key) of the object within the bucket.

    Returns:
        bytes: The binary content of the retrieved object.

    """
    s3_object = s3_client.get_object(Bucket=bucket, Key=filepath)

    body = s3_object["Body"].read()  # Be careful if object are too big, may need a loop

    if not body:
        raise ValueError(f"{filepath} in {bucket} is empty")

    return body


def send_object_to_s3(
    s3_client,
    bucket: str,
    object,
    s3_filepath: str,
):
    """Upload a file to an S3 bucket.

    Args:
        s3_client (boto3.client): An already initialized S3 client (see `connect_to_s3`).
        bucket (str): Name of the destination S3 bucket.
        object: Content of the file to upload (bytes, str, or an object compatible with `Body`).
        s3_filepath (str): Path (key) under which the object will be stored in the bucket.

    Returns:
        None

    """
    s3_client.put_object(Bucket=bucket, Body=object, Key=s3_filepath)


def get_folder_content_from_s3(s3_client: boto3.client, bucket_name: str, folder: str):
    """List the keys of objects present in a folder (prefix) of an S3 bucket.

    Args:
        s3_client (boto3.client): An already initialized S3 client (see `connect_to_s3`).
        bucket_name (str): Name of the S3 bucket to explore.
        folder (str): Prefix (folder path) used to filter objects.

    Returns:
        list[str]: List of keys (paths) of the objects found under the given prefix.

    """
    folder_list = [
        file["Key"]
        for file in s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)[
            "Contents"
        ]
    ]

    return folder_list
