import numpy as np
import polars as pl
from google.transit import gtfs_realtime_pb2


def sncf_trip_updates_protobuf_to_sheets(protobuf_sncf_data):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(protobuf_sncf_data)

    trip_list = []
    trip_ids = []

    for entity in feed.entity:
        trip_dict = {"trip_id": entity.id}
        trip_ids.append(entity.id)

        if entity.HasField("trip_update"):
            trip = entity.trip_update.trip
            stops = []

            for stop_time in entity.trip_update.stop_time_update:
                stop = {"stop_id": stop_time.stop_id}
                if stop_time.departure:
                    stop["departure"] = {
                        "time": stop_time.departure.time,
                        "delay": stop_time.departure.delay,
                    }

                if stop_time.arrival:
                    stop["arrival"] = {
                        "time": stop_time.arrival.time,
                        "delay": stop_time.arrival.delay,
                    }

                stops.append(stop)
        trip_dict["stops"] = stops

        trip_list.append(trip_dict)

    stop_updates = []

    for trip in trip_list:
        for stop in trip["stops"]:
            stop_update = [
                trip["trip_id"],
                stop["stop_id"],
                stop["departure"]["time"],
                stop["departure"]["delay"],
                stop["arrival"]["time"],
                stop["arrival"]["delay"],
            ]

            stop_updates.append(stop_update)

    stop_updates = np.array(stop_updates)

    trip_update_df = (
        pl.from_numpy(
            data=stop_updates,
            schema=[
                ("trip_id", pl.String),
                ("stop_id", pl.String),
                ("departure_time", pl.Int64),
                ("departure_delay", pl.Int64),
                ("arrival_time", pl.Int64),
                ("arrival_delay", pl.Int64),
            ],
        )
        .with_columns(
            pl.col("departure_time").replace(0, None),
            pl.col("arrival_time").replace(0, None),
        )
        .with_columns(
            pl.from_epoch("departure_time", time_unit="s"),
            pl.from_epoch("arrival_time", time_unit="s"),
        )
    )

    return trip_update_df


def transform_sncf_trip_updates_data(
    trip_updates_input_path: str, trip_updates_output_path: str
):
    trips_updates = sncf_trip_updates_protobuf_to_sheets(
        protobuf_file_path=trip_updates_input_path
    )

    trips_updates.write_parquet(trip_updates_output_path)

    return None
