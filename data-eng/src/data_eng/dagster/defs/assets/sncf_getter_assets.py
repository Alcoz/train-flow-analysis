import dagster as dg
from dotenv import load_dotenv
import os

from data_eng.utils.s3_connector import connect_to_s3, send_object_to_s3
from data_eng.sncf_getter import (
    get_sncf_theoretical_train_data,
    get_sncf_trip_update_train_data,
)
from data_eng.dagster.defs.partitions import daily_partitions


@dg.asset(partitions_def=daily_partitions)
def sncf_bronze_theoretical_data(context: dg.AssetExecutionContext):
    load_dotenv()
    os.environ["AWS_ENDPOINT_URL"] = os.getenv("MINIO_API")
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ACCESS_KEY")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_SECRET_ACCESS_KEY")

    THEORY_DATA_FOLDER = "data/{layer}/theory/"

    today = context.partition_key

    context.log.info(f"Processing date {today}")

    sncf_theoretical_data = get_sncf_theoretical_train_data()

    ########################################################

    s3_client = connect_to_s3(
        endpoint_url=os.getenv("MINIO_API"),
        access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        secret_access_key=os.getenv("MINIO_SECRET_ACCESS_KEY"),
        region_name=os.getenv("REGION_NAME"),
    )

    s3_bucket_name = os.getenv("BUCKET_NAME")

    send_object_to_s3(
        s3_client,
        bucket="sncf-bucket",
        object=sncf_theoretical_data["zip_file"],
        s3_filepath=f"raw/{today}/sncf_gtfs.zip",
    )

    for filename, file in sncf_theoretical_data["files"].items():
        send_object_to_s3(
            s3_client=s3_client,
            bucket=s3_bucket_name,
            object=file,
            s3_filepath=THEORY_DATA_FOLDER.format(layer="bronze")
            + f"date={today}/"
            + filename,
        )


@dg.asset(partitions_def=daily_partitions)
def sncf_bronze_continue_data(context: dg.AssetExecutionContext):
    load_dotenv()
    os.environ["AWS_ENDPOINT_URL"] = os.getenv("MINIO_API")
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ACCESS_KEY")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_SECRET_ACCESS_KEY")

    CONTINUE_DATA_FOLDER = "data/{layer}/continue/"

    today = context.partition_key

    sncf_trip_update_data = get_sncf_trip_update_train_data()

    s3_client = connect_to_s3(
        endpoint_url=os.getenv("MINIO_API"),
        access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        secret_access_key=os.getenv("MINIO_SECRET_ACCESS_KEY"),
        region_name=os.getenv("REGION_NAME"),
    )

    s3_bucket_name = os.getenv("BUCKET_NAME")

    send_object_to_s3(
        s3_client=s3_client,
        bucket=s3_bucket_name,
        object=sncf_trip_update_data,
        s3_filepath=CONTINUE_DATA_FOLDER.format(layer="bronze")
        + f"date={today}/"
        + "sncf_trip_update.pb",
    )
