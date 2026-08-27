from io import BytesIO

import dagster as dg
import polars as pl
from data_eng.sncf_transformer import (
    sncf_trip_updates_protobuf_to_sheets,
)
from data_eng.utils.s3_connector import get_folder_content_from_s3, get_object_from_s3
from orchestration_dagster.defs.partitions import daily_partitions
from orchestration_dagster.defs.resources import S3_Resource

GTFS_TABLES = [
    "trips",
    "transfers",
    "stops",
    "stop_times",
    "routes",
    "feed_info",
    "calendar_dates",
    "agency",
]


@dg.multi_asset(
    partitions_def=daily_partitions,
    outs={
        table: dg.AssetOut(key=["sncf_silver_theoretical_data", table])
        for table in GTFS_TABLES
    },
    deps=["sncf_bronze_theoretical_data"],
)
def sncf_silver_theoretical_data(
    context: dg.AssetExecutionContext, s3_resource: S3_Resource
):
    """Convert bronze theoretical GTFS files (CSV) into silver Parquet tables.

    Reads each CSV file present in the bronze folder for the current
    partition (trips, stops, routes, etc.), converts it into a Polars
    DataFrame, and writes it as Parquet in the silver layer. Files whose
    name doesn't match a known GTFS table (GTFS_TABLES) are skipped.

    Args:
    context: Dagster execution context, provides the partition key
        (date) and the logger.
    s3_resource: Dagster resource exposing the S3 client and target
        bucket.

    Yields:
    One MaterializeResult per GTFS table actually produced, with row
    count and S3 path as metadata.

    """
    THEORY_DATA_FOLDER = "data/{layer}/theory/"
    today = context.partition_key
    context.log.info(f"Processing date {today}")

    s3_client = s3_resource.get_client()
    s3_bucket_name = s3_resource.bucket_name

    results = {}

    for filename in get_folder_content_from_s3(
        s3_client=s3_client,
        bucket_name=s3_bucket_name,
        folder=THEORY_DATA_FOLDER.format(layer="bronze") + f"date={today}/",
    ):
        table_name = filename.split("/")[-1].removesuffix(".txt")

        if table_name not in GTFS_TABLES:
            context.log.warning(f"Fichier inattendu ignoré : {filename}")
            continue

        # Doit couvrir le cas où mon fichier n'existe pas
        file = get_object_from_s3(
            s3_client=s3_client, bucket=s3_bucket_name, filepath=filename
        )

        train_dataframe = pl.read_csv(
            file,
            separator=",",
            has_header=True,
            infer_schema_length=10_000,
            ignore_errors=True,
        )

        parquet_buffer = BytesIO()
        train_dataframe.write_parquet(parquet_buffer)
        parquet_buffer.seek(0)

        s3_key = (
            THEORY_DATA_FOLDER.format(layer="silver")
            + f"date={today}/{table_name}.parquet"
        )
        s3_client.upload_fileobj(
            parquet_buffer,
            s3_bucket_name,
            THEORY_DATA_FOLDER.format(layer="silver")
            + f"date={today}/"
            + f"{table_name}.parquet",
        )

        results[table_name] = dg.MaterializeResult(
            asset_key=["sncf_silver_theoretical_data", table_name],
            metadata={"rows": train_dataframe.height, "s3_path": s3_key},
        )

    # yield uniquement les tables effectivement produites
    for table_name in GTFS_TABLES:
        if table_name in results:
            yield results[table_name]


@dg.asset(deps=["sncf_bronze_continue_data"])
def sncf_silver_continue_data(
    context: dg.AssetExecutionContext, s3_resource: S3_Resource
):
    """Convert SNCF real-time trip updates (protobuf) into a silver Parquet table.

    Reads the GTFS-realtime protobuf file stored in bronze for the current
    partition, converts it into a DataFrame via
    `sncf_trip_updates_protobuf_to_sheets`, then writes the result as
    Parquet in the silver layer.

    Args:
    context: Dagster execution context, provides the partition key
        (date) and the logger.
    s3_resource: Dagster resource exposing the S3 client and target
        bucket.

    """
    CONTINUE_DATA_FOLDER = "data/{layer}/continue/"

    today = context.partition_key

    s3_client = s3_resource.get_client()
    s3_bucket_name = s3_resource.bucket_name

    protobuf_sncf_data = get_object_from_s3(
        s3_client=s3_client,
        bucket=s3_bucket_name,
        filepath=CONTINUE_DATA_FOLDER.format(layer="bronze")
        + f"date={today}/"
        + "sncf_trip_update.pb",
    )

    trips_updates_df = sncf_trip_updates_protobuf_to_sheets(
        protobuf_sncf_data=protobuf_sncf_data
    )

    parquet_buffer = BytesIO()

    trips_updates_df.write_parquet(parquet_buffer)

    parquet_buffer.seek(0)

    s3_client.upload_fileobj(
        parquet_buffer,
        s3_bucket_name,
        CONTINUE_DATA_FOLDER.format(layer="silver")
        + f"date={today}/"
        + "sncf_trip_update.parquet",
    )
