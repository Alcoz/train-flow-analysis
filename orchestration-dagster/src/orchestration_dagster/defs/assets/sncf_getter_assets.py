from datetime import datetime

import dagster as dg
from data_eng.sncf_getter import (
    EXPECTED_GTFS_FILES,
    find_missing_gtfs_files,
    get_sncf_theoretical_train_data,
    get_sncf_trip_update_train_data,
)
from data_eng.utils.s3_connector import send_object_to_s3
from orchestration_dagster.defs.resources import S3_Resource
from pytz import timezone


@dg.asset(
    check_specs=[
        dg.AssetCheckSpec(
            name="expected_gtfs_files_present",
            asset="sncf_bronze_theoretical_data",
            description="The GTFS files used downstream are in the archive and not empty.",
        )
    ]
)
def sncf_bronze_theoretical_data(
    context: dg.AssetExecutionContext, s3_resource: S3_Resource
) -> dg.MaterializeResult:
    """Extract and store SNCF theoretical data (GTFS) in the bronze layer.

    Fetches the theoretical GTFS zip archive from the SNCF API and uploads it
    unchanged to S3. Its files are extracted in the silver layer. Missing
    expected files are reported by a non-blocking asset check: the archive is
    stored whatever its content.

    Args:
        context: Dagster execution context, provides the logger.
        s3_resource: Dagster resource exposing the S3 client and target
            bucket.

    Returns:
        The asset materialization result, with the expected files check.

    """
    today = datetime.now(tz=timezone("Europe/Paris")).strftime("%Y-%m-%d")
    s3_filepath = f"data/bronze/theory/date={today}/sncf_gtfs.zip"

    context.log.info(f"Processing date {today}")

    try:
        sncf_theoretical_zip = get_sncf_theoretical_train_data()
    except Exception as e:
        context.log.error(f"Error while getting theoretical data : {e}")
        raise

    s3_client = s3_resource.get_client()
    s3_bucket_name = s3_resource.bucket_name

    try:
        send_object_to_s3(
            s3_client,
            bucket=s3_bucket_name,
            object=sncf_theoretical_zip,
            s3_filepath=s3_filepath,
        )
    except Exception as e:
        context.log.error(
            f"Error while sending raw theoretical file to s3 bucket : {e}"
        )
        raise

    context.log.info(
        f"Raw archive of {today} is saved on s3 bucket {s3_bucket_name} at {s3_filepath} successfully"
    )

    missing_files = find_missing_gtfs_files(sncf_theoretical_zip, EXPECTED_GTFS_FILES)
    if missing_files:
        context.log.warning(f"Missing or empty GTFS files: {missing_files}")

    return dg.MaterializeResult(
        metadata={"s3_path": s3_filepath},
        check_results=[
            dg.AssetCheckResult(
                check_name="expected_gtfs_files_present",
                passed=not missing_files,
                severity=dg.AssetCheckSeverity.WARN,
                metadata={"missing_files": missing_files},
            )
        ],
    )


@dg.asset()
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

    now = datetime.now(tz=timezone("Europe/Paris"))
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    now_hms = now.strftime("%H-%M-%S")

    context.log.info(f"Processing date {now}")

    try:
        sncf_trip_update_data = get_sncf_trip_update_train_data()
    except Exception as e:
        context.log.error(f"Error while getting theoretical data : {e}")
        raise

    s3_client = s3_resource.get_client()
    s3_bucket_name = s3_resource.bucket_name
    s3_filepath = (
        CONTINUE_DATA_FOLDER.format(layer="bronze")
        + f"year={year}/"
        + f"month={month}/"
        + f"day={day}/"
        + f"{now_hms}.pb"
    )

    try:
        send_object_to_s3(
            s3_client=s3_client,
            bucket=s3_bucket_name,
            object=sncf_trip_update_data,
            s3_filepath=s3_filepath,
        )
    except Exception as e:
        context.log.error(
            f"Error while sending raw theoretical file to s3 bucket : {e}"
        )
        raise

    context.log.info(
        f"Raw file of {now} is saved on s3 bucket {s3_bucket_name} at {s3_filepath} successfully"
    )
