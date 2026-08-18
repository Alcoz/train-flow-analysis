import dagster as dg
from data_eng.sncf_getter import (
    get_sncf_theoretical_train_data,
    get_sncf_trip_update_train_data,
)
from data_eng.utils.s3_connector import send_object_to_s3
from orchestration_dagster.defs.partitions import daily_partitions
from orchestration_dagster.defs.resources import S3_Resource


@dg.asset(partitions_def=daily_partitions)
def sncf_bronze_theoretical_data(
    context: dg.AssetExecutionContext, s3_resource: S3_Resource
) -> dg.MaterializeResult:
    """Extract and store SNCF theoretical data (GTFS) in the bronze layer.

    Fetches the theoretical GTFS export (zip archive and individual files)
    from the SNCF API for the current partition date, then uploads the raw
    archive and each extracted file to S3.

    Args:
        context: Dagster execution context, provides the partition key
            (date) and the logger.
        s3_resource: Dagster resource exposing the S3 client and target
            bucket.

    Returns:
        The asset materialization result.

    """
    THEORY_DATA_FOLDER = "data/{layer}/theory/"

    today = context.partition_key

    context.log.info(f"Processing date {today}")

    sncf_theoretical_data = get_sncf_theoretical_train_data()

    s3_client = s3_resource.get_client()
    s3_bucket_name = s3_resource.bucket_name

    send_object_to_s3(
        s3_client,
        bucket=s3_bucket_name,
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
    """Extract and store SNCF real-time trip updates in the bronze layer.

    Fetches the GTFS-realtime feed (trip updates) from SNCF for the current
    partition date and uploads the raw protobuf file to S3.

    Args:
    context: Dagster execution context, provides the partition key
        (date) and the logger.
    s3_resource: Dagster resource exposing the S3 client and target
        bucket.

    """
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
