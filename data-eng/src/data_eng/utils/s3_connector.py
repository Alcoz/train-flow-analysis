import boto3


def connect_to_s3(
    endpoint_url: str, access_key_id: str, secret_access_key: str, region_name: str
):
    """Créer un client de connexion avec un S3"""
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region_name,
        )
        return s3_client
    except Exception as e:
        print(f"Erreur de connexion : {e}")
        return None


def send_local_file_to_s3(
    s3_client: boto3.client,
    bucket: str,
    filepath: str,
    s3_filepath: str,
):
    """Dépose un fichier sur un bucket S3"""
    with open(filepath, "rb") as f:  # ✅ Ouvrir en mode binaire
        s3_client.put_object(Bucket=bucket, Body=f, Key=s3_filepath, ACL="public-read")


def send_file_to_s3(
    s3_client: boto3.client,
    bucket: str,
    object,
    s3_filepath: str,
):
    """Dépose un fichier sur un bucket S3"""
    s3_client.put_object(Bucket=bucket, Body=object, Key=s3_filepath, ACL="public-read")
