# Silver tables

What each Silver table holds: where it is stored, what one row is, and its columns.
Silver is written by the Dagster assets in
[`sncf_transfo_silver_assets.py`](../../orchestration-dagster/src/orchestration_dagster/defs/assets/sncf_transfo_silver_assets.py),
with the conversion functions of [`sncf_transformer.py`](../src/data_eng/sncf_transformer.py).
Column types below come from that code.

| Table | Path in `s3://sncf-bucket/data/silver/` | One row = | Partition = | Read by (dbt) |
| --- | --- | --- | --- | --- |
| [`trips`](#trips) | `theory/date=D/trips.parquet` | scheduled trip of the GTFS version | GTFS publication day | `int_planned_trains` |
| [`calendar_dates`](#calendar_dates) | `theory/date=D/calendar_dates.parquet` | day a `service_id` runs | GTFS publication day | `int_planned_trains` |
| [`routes`](#routes) | `theory/date=D/routes.parquet` | line | GTFS publication day | `dim_routes` |
| [`stops`](#stops) | `theory/date=D/stops.parquet` | stop area or stop point | GTFS publication day | — |
| [`stop_times`](#other-gtfs-tables), `transfers`, `feed_info`, `agency` | `theory/date=D/<table>.parquet` | see the GTFS reference | GTFS publication day | — |
| [`trip_updates`](#trip_updates) | `continue/date=D/<fetch day>_<HH-MM-SS>.parquet` | snapshot × trip × stop | **service date** | `fact_trip_runs` |
| [`alerts`](#alerts) | `alerts/date=D/<HH-MM-SS>.parquet` | snapshot × alert | **fetch day** | `dim_alerts` |

Rules shared by every table:

- Partitions are days in `Europe/Paris`. Rebuilding a partition overwrites the same files.
- Timestamps are **naive UTC** (no time zone in the Parquet type).
- The `date=` folder is not a column of the files: dbt reads it from the file name (`filename = true`).

## Theoretical GTFS (`theory/`)

The SNCF GTFS archive of the day, one Parquet file per table, columns as published.
Every column is a **string**, identifiers included, except the ones typed below
(strict casts: an unexpected value fails the run instead of becoming null).
Columns depend on what the SNCF publishes: those listed, and the "empty" ones, were checked on the GTFS of 2026-07-28.

### trips

| Column | Type | Description |
| --- | --- | --- |
| `trip_id` | string | Trip of this GTFS version. Its first part is the train number (`OCESN862402F…`). |
| `route_id` | string | Line of the trip. |
| `service_id` | string | Days the trip runs, given by `calendar_dates`. |
| `trip_headsign` | string | Train number shown to travellers (e.g. `862402`). |
| `direction_id` | int | 0 or 1. |
| `block_id` | string | Trips run by the same rolling stock. |
| `shape_id` | string | Empty. |

### calendar_dates

| Column | Type | Description |
| --- | --- | --- |
| `service_id` | string | Key to `trips.service_id`. |
| `date` | date | Day the service runs (service day, not the GTFS version). |
| `exception_type` | int | 1 = runs that day, 2 = removed that day. |

### routes

| Column | Type | Description |
| --- | --- | --- |
| `route_id` | string | Line (e.g. `FR:Line::00F2577A-…:`). |
| `agency_id` | string | Key to `agency`. |
| `route_short_name` | string | Short name (e.g. `P53`). |
| `route_long_name` | string | `Origin - Destination`, parsed into terminal stations by `dim_routes`. |
| `route_desc`, `route_url` | string | Empty. |
| `route_type` | int | GTFS mode: 2 = rail, 3 = bus, 0 = tram. |
| `route_color`, `route_text_color` | string | Hex colors, often empty. |

### stops

| Column | Type | Description |
| --- | --- | --- |
| `stop_id` | string | `StopArea:…` (station) or `StopPoint:…` (platform of a mode). |
| `stop_name` | string | Station name. |
| `stop_lat`, `stop_lon` | float | Coordinates. |
| `location_type` | int | 1 = stop area (station), 0 = stop point. |
| `parent_station` | string | Stop area of a stop point, empty for stop areas. |
| `stop_desc`, `zone_id`, `stop_url` | string | Empty. |

### Other GTFS tables

Converted but read by no model: `stop_times` (planned times per trip and stop,
typed `stop_sequence`, `pickup_type`, `drop_off_type`, `shape_dist_traveled`, `timepoint`),
`transfers` (`transfer_type`, `min_transfer_time`), `feed_info`
(`feed_start_date`, `feed_end_date` as dates, `feed_version`) and `agency`.

## trip_updates (`continue/`)

Flattened GTFS-RT trip updates, fetched every 2 minutes. The partition is the
**service date** (`start_date`), not the fetch day: the partition D reads the
snapshots of D and D+1, so overnight trains are complete. A trip appears once per
snapshot it is in: picking its last state is left to `fact_trip_runs`.

| Column | Type | Description |
| --- | --- | --- |
| `trip_id` | string | Usually the GTFS trip_id. Can also be a short train number (`OCESN19812F`), and trains added in real time have one that is not in the GTFS: `fact_trip_runs` matches them by train number. |
| `start_date` | date | Service date of the train. |
| `trip_schedule_relationship` | string | `SCHEDULED`, `ADDED` or `CANCELED`. |
| `stop_id` | string | Stop point. |
| `stop_position` | int | Position of the stop in the list sent by the SNCF (0 = origin). The SNCF never sends `stop_sequence`: this order is the actual one, added stops included. |
| `stop_schedule_relationship` | string | `SCHEDULED` or `SKIPPED` (stop not served). |
| `departure_time`, `arrival_time` | timestamp | Forecast or actual time, null when not sent. |
| `departure_delay`, `arrival_delay` | int | Delay in seconds, null when not sent. |
| `fetched_at` | timestamp | Time of the snapshot. |

## alerts (`alerts/`)

GTFS-RT service alerts, fetched every 2 minutes, one file per snapshot. Alerts
have no service date (an active period can span weeks), so the partition is the
**fetch day**. An alert is repeated in every snapshot it appears in: picking its
last version of the day is left to `dim_alerts`.

| Column | Type | Description |
| --- | --- | --- |
| `alert_id` | string | Stays the same across snapshots and days. |
| `cause`, `effect` | string | GTFS-RT enum names, **null when the SNCF did not set them** (not `UNKNOWN_CAUSE`). |
| `header_text`, `description_text` | string | French text (first language when there is no French), HTML removed. |
| `active_periods` | list of struct(`start`, `end` timestamp) | A null start or end means open on that side. |
| `informed_entities` | list of struct(`trip_id`, `route_id`, `stop_id` string) | Targets. `trip_id` is a **train number** (`OCESN862402F`), not a GTFS trip_id. |
| `fetched_at` | timestamp | Time of the snapshot. |
