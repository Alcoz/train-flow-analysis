from datetime import datetime, timedelta
from io import BytesIO

import dagster as dg
import polars as pl
from data_eng.sncf_transformer import (
    gtfs_csv_to_dataframe,
    sncf_trip_updates_protobuf_to_sheets,
)
from data_eng.utils.s3_connector import get_folder_content_from_s3, get_object_from_s3
from orchestration_dagster.defs.partitions import daily_partitions
from orchestration_dagster.defs.resources import S3_Resource

# table_name, is_mandatory
GTFS_TABLES = [
    ["trips", True],
    ["transfers", False],
    ["stops", True],
    ["stop_times", False],
    ["routes", True],
    ["feed_info", False],
    ["calendar_dates", False],
    ["agency", False],
]


@dg.multi_asset(
    partitions_def=daily_partitions,
    outs={
        table: dg.AssetOut(
            key=["sncf_silver_theoretical_data", table], is_required=is_required
        )
        for table, is_required in GTFS_TABLES
    },
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
    today = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    context.log.info(f"Processing date {today}")

    s3_client = s3_resource.get_client()
    s3_bucket_name = s3_resource.bucket_name

    gtfs_table_names = [row[0] for row in GTFS_TABLES]

    results = {}

    for filename in get_folder_content_from_s3(
        s3_client=s3_client,
        bucket_name=s3_bucket_name,
        folder=THEORY_DATA_FOLDER.format(layer="bronze") + f"date={today}/",
    ):
        table_name = filename.split("/")[-1].removesuffix(".txt")
        context.log.info(f"Processeing table : {table_name}")

        if table_name not in gtfs_table_names:
            context.log.warning(f"Fichier inattendu ignoré : {filename}")
            continue

        # Doit couvrir le cas où mon fichier n'existe pas
        file = get_object_from_s3(
            s3_client=s3_client,
            bucket=s3_bucket_name,
            filepath=THEORY_DATA_FOLDER.format(layer="bronze")
            + f"date={today}/"
            + filename,
        )

        train_dataframe = gtfs_csv_to_dataframe(table_name, file)

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
    for table_name in gtfs_table_names:
        if table_name in results:
            yield results[table_name]


@dg.asset(partitions_def=daily_partitions)
def sncf_silver_continue_data(
    context: dg.AssetExecutionContext, s3_resource: S3_Resource
):
    """Convert SNCF real-time trip updates (protobuf) into silver Parquet files.

    Silver real-time data is stored by service date (the trip start_date),
    not by fetch day: trains that started on the partition day keep being
    updated after midnight. The partition day and the following fetch day
    are both read from bronze, and only the rows of trips started on the
    partition day are kept. Each bronze file of a fetch day is therefore
    converted twice, once for each service date it holds.

    Files are written under deterministic names, so processing a partition
    again only overwrites them.

    Args:
    context: Dagster execution context, provides the partition key
        (date) and the logger.
    s3_resource: Dagster resource exposing the S3 client and target
        bucket.

    """
    CONTINUE_DATA_FOLDER = "data/{layer}/continue/"

    service_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    silver_folder = CONTINUE_DATA_FOLDER.format(layer="silver") + (
        f"service_date={service_date}/"
    )

    s3_client = s3_resource.get_client()
    s3_bucket_name = s3_resource.bucket_name

    rows = 0
    for fetch_day in (service_date, service_date + timedelta(days=1)):
        bronze_folder = CONTINUE_DATA_FOLDER.format(
            layer="bronze"
        ) + fetch_day.strftime("year=%Y/month=%m/day=%d/")
        context.log.info(f"Processing fetch day {fetch_day}")

        for filename in get_folder_content_from_s3(
            s3_client=s3_client,
            bucket_name=s3_bucket_name,
            folder=bronze_folder,
        ):
            protobuf_sncf_data = get_object_from_s3(
                s3_client=s3_client,
                bucket=s3_bucket_name,
                filepath=bronze_folder + filename,
            )

            trips_updates_df = sncf_trip_updates_protobuf_to_sheets(
                protobuf_sncf_data=protobuf_sncf_data
            ).filter(pl.col("start_date") == service_date)

            if trips_updates_df.is_empty():
                continue

            parquet_buffer = BytesIO()
            trips_updates_df.write_parquet(parquet_buffer)
            parquet_buffer.seek(0)

            # Bronze file names are only unique within a fetch day
            s3_client.upload_fileobj(
                parquet_buffer,
                s3_bucket_name,
                silver_folder + f"{fetch_day}_{filename.split('.')[0]}.parquet",
            )
            rows += trips_updates_df.height

    return dg.MaterializeResult(metadata={"rows": rows, "s3_path": silver_folder})
