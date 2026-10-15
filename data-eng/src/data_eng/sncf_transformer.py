"""Transformation functions for SNCF data."""

import html
import io
import re
import zipfile
from datetime import UTC, datetime

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

# GTFS-RT statuses, stored by name: the feed only sends them when they differ
# from SCHEDULED, an absent field reads as its default (0 = SCHEDULED).
TRIP_SCHEDULE_RELATIONSHIP = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship
STOP_SCHEDULE_RELATIONSHIP = (
    gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship
)

# Unlike the statuses above, an absent alert cause or effect is kept as null:
# its default (UNKNOWN_CAUSE) would hide that the SNCF did not set it.
ALERT_CAUSE = gtfs_realtime_pb2.Alert.Cause
ALERT_EFFECT = gtfs_realtime_pb2.Alert.Effect


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
        trip_schedule_relationship = TRIP_SCHEDULE_RELATIONSHIP.Name(
            entity.trip_update.trip.schedule_relationship
        )

        # The SNCF never sends stop_sequence: the order of the list is the actual
        # order of the trip (added stops included), and passed stops stay in it.
        for stop_position, stop_time in enumerate(entity.trip_update.stop_time_update):
            dep, arr = stop_time.departure, stop_time.arrival
            rows.append(
                {
                    "trip_id": trip_id,
                    "start_time": trip_start_time,
                    "start_date": f"{trip_start_date[0:4]}-{trip_start_date[4:6]}-{trip_start_date[6:8]}",
                    "trip_schedule_relationship": trip_schedule_relationship,
                    "stop_id": stop_time.stop_id,
                    "stop_position": stop_position,
                    "stop_schedule_relationship": STOP_SCHEDULE_RELATIONSHIP.Name(
                        stop_time.schedule_relationship
                    ),
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
        "trip_schedule_relationship": pl.String,
        "stop_id": pl.String,
        "stop_position": pl.Int64,
        "stop_schedule_relationship": pl.String,
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


def _french_text(translated_string) -> str | None:
    """Return the French translation of a GTFS-RT text, cleaned of its HTML.

    Alerts come in up to 6 languages; the classification rules are written
    for French. Falls back to the first translation when there is no French.
    Descriptions hold HTML (<p>, &nbsp;...): tags are removed, entities
    decoded and whitespace collapsed.
    """
    translations = translated_string.translation
    if not translations:
        return None
    text = next((t.text for t in translations if t.language == "fr"), None)
    if text is None:
        text = translations[0].text
    text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    return re.sub(r"\s+", " ", text).strip() or None


def _utc_datetime(timestamp: int) -> datetime | None:
    """Convert a POSIX timestamp to a naive UTC datetime, 0 (unset) to None."""
    if not timestamp:
        return None
    return datetime.fromtimestamp(timestamp, UTC).replace(tzinfo=None)


def sncf_service_alerts_protobuf_to_sheets(protobuf_sncf_data: bytes) -> pl.DataFrame:
    """Transform a GTFS-RT service alerts snapshot into a dataframe.

    One row per alert. An alert can target several trains and have several
    active periods: both are kept as list columns.

    Args:
        protobuf_sncf_data (bytes): service alerts snapshot in GTFS-RT format.

    Returns:
        pl.DataFrame: dataframe containing one row per alert

    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(protobuf_sncf_data)

    fetched_at = feed.header.timestamp
    rows = []

    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue

        alert = entity.alert
        rows.append(
            {
                "alert_id": entity.id,
                "cause": ALERT_CAUSE.Name(alert.cause)
                if alert.HasField("cause")
                else None,
                "effect": ALERT_EFFECT.Name(alert.effect)
                if alert.HasField("effect")
                else None,
                "header_text": _french_text(alert.header_text),
                "description_text": _french_text(alert.description_text),
                # An unset start or end means the period is open on that side
                "active_periods": [
                    {"start": _utc_datetime(p.start), "end": _utc_datetime(p.end)}
                    for p in alert.active_period
                ],
                # trip_id is a train number (e.g. OCESN862402F), not a GTFS trip_id
                "informed_entities": [
                    {
                        "trip_id": e.trip.trip_id or None,
                        "route_id": e.route_id or None,
                        "stop_id": e.stop_id or None,
                    }
                    for e in alert.informed_entity
                ],
                "fetched_at": fetched_at,
            }
        )

    schema = {
        "alert_id": pl.String,
        "cause": pl.String,
        "effect": pl.String,
        "header_text": pl.String,
        "description_text": pl.String,
        "active_periods": pl.List(
            pl.Struct({"start": pl.Datetime("us"), "end": pl.Datetime("us")})
        ),
        "informed_entities": pl.List(
            pl.Struct(
                {"trip_id": pl.String, "route_id": pl.String, "stop_id": pl.String}
            )
        ),
        "fetched_at": pl.Int64,
    }

    return pl.DataFrame(rows, schema=schema).with_columns(
        pl.from_epoch("fetched_at", time_unit="s"),
    )
