import dagster as dg

from data_eng.utils.s3_connector import send_object_to_s3
from data_eng.sncf_getter import (
    get_sncf_theoretical_train_data,
    get_sncf_trip_update_train_data,
)
from orchestration_dagster.defs.partitions import daily_partitions
from orchestration_dagster.defs.resources import S3_Resource


@dg.asset(partitions_def=daily_partitions)
def sncf_bronze_theoretical_data(
    context: dg.AssetExecutionContext, s3_resource: S3_Resource
) -> dg.MaterializeResult:
    THEORY_DATA_FOLDER = "data/{layer}/theory/"

    today = context.partition_key

    context.log.info(f"Processing date {today}")

    sncf_theoretical_data = get_sncf_theoretical_train_data()

    s3_client = s3_resource.get_client()
    s3_bucket_name = s3_resource.bucket_name

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
def sncf_bronze_continue_data(
    context: dg.AssetExecutionContext, s3_resource: S3_Resource
):
    CONTINUE_DATA_FOLDER = "data/{layer}/continue/"

    today = context.partition_key

    sncf_trip_update_data = get_sncf_trip_update_train_data()

    s3_client = s3_resource.get_client()

    s3_bucket_name = s3_resource.bucket_name

    send_object_to_s3(
        s3_client=s3_client,
        bucket=s3_bucket_name,
        object=sncf_trip_update_data,
        s3_filepath=CONTINUE_DATA_FOLDER.format(layer="bronze")
        + f"date={today}/"
        + "sncf_trip_update.pb",
    )
