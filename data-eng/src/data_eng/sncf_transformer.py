"""Transformation functions for SNCF data."""

import polars as pl
from google.transit import gtfs_realtime_pb2


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
                    "start_date": trip_start_date,
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
        pl.from_epoch("departure_time", time_unit="s"),
        pl.from_epoch("arrival_time", time_unit="s"),
        pl.from_epoch("fetched_at", time_unit="s"),
    )
