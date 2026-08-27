import os

import pytest
from data_eng.utils.s3_connector import connect_to_s3, get_object_from_s3
from moto import mock_aws


@pytest.fixture
def aws_credentials():
    """Mock AWS credentials for moto."""
    import os

    os.environ["S3_ACCESS_KEY"] = "testing"
    os.environ["S3_SECRET_ACCESS_KEY"] = "testing"
    os.environ["REGION_NAME"] = "eu-west-1"
    os.environ["BUCKET_NAME"] = "test-bucket"


@pytest.fixture
def s3_client():
    """Fournit un client S3 mocké pour tous les tests qui en ont besoin."""
    with mock_aws():
        client = connect_to_s3(
            endpoint_url=None,
            access_key_id=os.getenv("S3_ACCESS_KEY"),
            secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
            region_name=os.getenv("REGION_NAME"),
        )

        client.create_bucket(
            Bucket=os.getenv("BUCKET_NAME"),
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        yield client


@mock_aws
def test_get_object_from_s3(aws_credentials, s3_client):
    """Check if get object from s3 bucket behaviour works."""
    s3_client.put_object(
        Bucket="test-bucket",
        Key="data/test.json",
        Body=b'{"key": "value"}',
    )

    data = get_object_from_s3(
        s3_client=s3_client,
        bucket=os.getenv("BUCKET_NAME"),
        filepath="data/test.json",
    )

    assert data


@mock_aws
def test_get_empty_object_from_s3(aws_credentials, s3_client):
    """Check if get object from s3 bucket behaviour works."""
    s3_client.put_object(
        Bucket="test-bucket",
        Key="data/test.json",
        Body=b"",
    )

    with pytest.raises(ValueError, match="data/test.json in test-bucket is empty"):
        get_object_from_s3(
            s3_client=s3_client,
            bucket=os.getenv("BUCKET_NAME"),
            filepath="data/test.json",
        )
