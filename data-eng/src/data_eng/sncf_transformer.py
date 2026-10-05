"""Transformation functions for SNCF data."""

import io
import zipfile

import polars as pl
from google.transit import gtfs_realtime_pb2

# Non-text columns of the GTFS tables, typed as in the GTFS reference.
# Every other column (all identifiers included) stays a string: inferring
# types turned ids such as service_id or trip_headsign into integers.
GTFS_COLUMN_TYPES = {
    "trips": {
        "direction_id": pl.Int64,
        "wheelchair_accessible": pl.Int64,
        "bikes_allowed": pl.Int64,
    },
    "routes": {
        "route_type": pl.Int64,
        "route_sort_order": pl.Int64,
    },
    "stops": {
        "stop_lat": pl.Float64,
        "stop_lon": pl.Float64,
        "location_type": pl.Int64,
        "wheelchair_boarding": pl.Int64,
    },
    "stop_times": {
        "stop_sequence": pl.Int64,
        "pickup_type": pl.Int64,
        "drop_off_type": pl.Int64,
        "shape_dist_traveled": pl.Float64,
        "timepoint": pl.Int64,
    },
    "transfers": {
        "transfer_type": pl.Int64,
        "min_transfer_time": pl.Int64,
    },
    "calendar_dates": {
        "date": pl.Date,
        "exception_type": pl.Int64,
    },
    "feed_info": {
        "feed_start_date": pl.Date,
        "feed_end_date": pl.Date,
    },
}


def extract_gtfs_files(zip_bytes: bytes) -> dict[str, bytes]:
    """Extract the files of a GTFS zip archive.

    Args:
        zip_bytes (bytes): the GTFS zip archive.

    Returns:
        dict[str, bytes]: content of each file, by table name (file name
            without the .txt extension). Folders are skipped.

    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        return {
            filename.split("/")[-1].removesuffix(".txt"): archive.read(filename)
            for filename in archive.namelist()
            if not filename.endswith("/")
        }


def gtfs_csv_to_dataframe(table_name: str, csv_content: bytes) -> pl.DataFrame:
    """Read a GTFS table (CSV) with explicit column types.

    Columns are read as strings, then the known non-text columns are cast.
    Casts are strict: an unexpected value fails instead of becoming null.

    Args:
        table_name (str): GTFS table name, without extension (e.g. "trips").
        csv_content (bytes): content of the GTFS .txt file.

    Returns:
        pl.DataFrame: the typed table.

    """
    df = pl.read_csv(csv_content, infer_schema=False)

    casts = []
    for column, dtype in GTFS_COLUMN_TYPES.get(table_name, {}).items():
        if column not in df.columns:
            continue
        if dtype == pl.Date:
            # GTFS dates are written YYYYMMDD
            casts.append(pl.col(column).str.to_date("%Y%m%d"))
        else:
            casts.append(pl.col(column).cast(dtype, strict=True))

    return df.with_columns(casts)


def sncf_trip_updates_protobuf_to_sheets(protobuf_sncf_data):
    """Transform gtfs-rt format of trip updates data in dataframe format.

    Args:
        protobuf_sncf_data (_type_): trip update sncf data in gtfs-rt format.

    Returns:
        pl.DataFrame: dataframe containing trip updates data

    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(protobuf_sncf_data)

    fetched_at = feed.header.timestamp
    rows = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_id = entity.trip_update.trip.trip_id
        trip_start_time = entity.trip_update.trip.start_time
        trip_start_date = entity.trip_update.trip.start_date

        for stop_time in entity.trip_update.stop_time_update:
            dep, arr = stop_time.departure, stop_time.arrival
            rows.append(
                {
                    "trip_id": trip_id,
                    "start_time": trip_start_time,
                    "start_date": f"{trip_start_date[0:4]}-{trip_start_date[4:6]}-{trip_start_date[6:8]}",
                    "stop_id": stop_time.stop_id,
                    "departure_time": dep.time
                    if dep.HasField("time") and dep.time
                    else None,
                    "departure_delay": dep.delay if dep.HasField("delay") else None,
                    "arrival_time": arr.time
                    if arr.HasField("time") and arr.time
                    else None,
                    "arrival_delay": arr.delay if arr.HasField("delay") else None,
                    "fetched_at": fetched_at,
                }
            )

    schema = {
        "trip_id": pl.String,
        "start_date": pl.String,
        "stop_id": pl.String,
        "stop_sequence": pl.Int64,
        "schedule_relationship": pl.Int64,
        "departure_time": pl.Int64,
        "departure_delay": pl.Int64,
        "arrival_time": pl.Int64,
        "arrival_delay": pl.Int64,
        "fetched_at": pl.Int64,
    }

    return pl.DataFrame(rows, schema=schema).with_columns(
        pl.from_epoch("start_date", time_unit="d"),
        pl.from_epoch("departure_time", time_unit="s"),
        pl.from_epoch("arrival_time", time_unit="s"),
        pl.from_epoch("fetched_at", time_unit="s"),
    )
