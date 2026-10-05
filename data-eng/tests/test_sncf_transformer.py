import polars as pl
import pytest
from data_eng.sncf_transformer import gtfs_csv_to_dataframe


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
