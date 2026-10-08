import io
import zipfile

import polars as pl
import pytest
from data_eng.sncf_transformer import (
    extract_gtfs_files,
    gtfs_csv_to_dataframe,
    sncf_trip_updates_protobuf_to_sheets,
)
from google.transit import gtfs_realtime_pb2


def test_gtfs_ids_stay_strings():
    """Numeric-looking identifiers must not be inferred as integers."""
    df = gtfs_csv_to_dataframe(
        "trips",
        b"route_id,service_id,trip_id,trip_headsign,direction_id\n"
        b"R1,0042,T1,883100,1\n",
    )

    assert df.schema["service_id"] == pl.String
    assert df["service_id"][0] == "0042"
    assert df.schema["trip_headsign"] == pl.String
    assert df.schema["direction_id"] == pl.Int64


def test_gtfs_dates_and_empty_values():
    """GTFS dates are parsed and empty values become nulls."""
    df = gtfs_csv_to_dataframe(
        "calendar_dates",
        b"service_id,date,exception_type\n1,20260922,1\n2,20260923,\n",
    )

    assert df.schema["date"] == pl.Date
    assert df["exception_type"].to_list() == [1, None]


def test_gtfs_unexpected_value_fails():
    """A value that does not match the GTFS type fails instead of becoming null."""
    with pytest.raises(pl.exceptions.InvalidOperationError):
        gtfs_csv_to_dataframe("trips", b"trip_id,direction_id\nT1,north\n")


def test_gtfs_unknown_table_is_all_strings():
    """Tables without declared types are read as strings."""
    df = gtfs_csv_to_dataframe("agency", b"agency_id,agency_name\n1,SNCF\n")

    assert set(df.schema.values()) == {pl.String}


def test_extract_gtfs_files():
    """Files are returned by table name, folders are skipped."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("trips.txt", b"trip_id\nT1")
        z.writestr("extra/", b"")
        z.writestr("extra/stops.txt", b"stop_id\nS1")

    assert extract_gtfs_files(buffer.getvalue()) == {
        "trips": b"trip_id\nT1",
        "stops": b"stop_id\nS1",
    }


def test_trip_updates_schedule_relationships():
    """Trip and stop statuses are kept by name, absent ones read as SCHEDULED."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "1.0"
    feed.header.timestamp = 1_790_000_000

    # Normal trip: the feed does not send the statuses
    running = feed.entity.add(id="1").trip_update
    running.trip.trip_id = "T1"
    running.trip.start_date = "20260922"
    running.stop_time_update.add(stop_id="A")
    running.stop_time_update.add(
        stop_id="B",
        schedule_relationship=gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED,
    )

    # Cancelled trip: every stop is skipped
    cancelled = feed.entity.add(id="2").trip_update
    cancelled.trip.trip_id = "T2"
    cancelled.trip.start_date = "20260922"
    cancelled.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
    cancelled.stop_time_update.add(
        stop_id="A",
        schedule_relationship=gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED,
    )

    df = sncf_trip_updates_protobuf_to_sheets(feed.SerializeToString())

    assert df.select(
        "trip_id", "stop_id", "trip_schedule_relationship", "stop_schedule_relationship"
    ).rows() == [
        ("T1", "A", "SCHEDULED", "SCHEDULED"),
        ("T1", "B", "SCHEDULED", "SKIPPED"),
        ("T2", "A", "CANCELED", "SKIPPED"),
    ]


def test_trip_updates_stop_position():
    """Stops are numbered from 0 in the order of the feed, for each trip."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "1.0"
    feed.header.timestamp = 1_790_000_000

    for trip_id, stop_ids in [("T1", ["C", "A", "B"]), ("T2", ["A", "B"])]:
        trip_update = feed.entity.add(id=trip_id).trip_update
        trip_update.trip.trip_id = trip_id
        trip_update.trip.start_date = "20260922"
        for stop_id in stop_ids:
            trip_update.stop_time_update.add(stop_id=stop_id)

    df = sncf_trip_updates_protobuf_to_sheets(feed.SerializeToString())

    assert df.select("trip_id", "stop_id", "stop_position").rows() == [
        ("T1", "C", 0),
        ("T1", "A", 1),
        ("T1", "B", 2),
        ("T2", "A", 0),
        ("T2", "B", 1),
    ]
