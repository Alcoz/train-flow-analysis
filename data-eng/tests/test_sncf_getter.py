# Check the downloads: zip not empty, not corrupted, and the report of missing files
import io
import zipfile
from unittest.mock import MagicMock, patch

import pytest
from data_eng.sncf_getter import (
    EmptyRequiredFileError,
    find_missing_gtfs_files,
    get_sncf_theoretical_train_data,
    get_sncf_trip_update_train_data,
)


def build_fake_zip(files: dict) -> bytes:
    """Construit un vrai zip en mémoire à partir d'un dict {nom_fichier: contenu_bytes}."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        for filename, content in files.items():
            z.writestr(filename, content)
    return buffer.getvalue()


def mock_download(content: bytes) -> MagicMock:
    """Build a fake requests response returning the given content."""
    mock_response = MagicMock()
    mock_response.content = content
    mock_response.raise_for_status = MagicMock()  # ne lève rien
    return mock_response


def test_theoretical_archive_returned_as_is():
    """The getter returns the downloaded archive without filtering its files."""
    fake_zip_bytes = build_fake_zip(
        {
            "trips.txt": b"trip_id,route_id\n1,A",
            "chemin.txt": b"route_id\n2,B",
        }
    )

    with patch("requests.get", return_value=mock_download(fake_zip_bytes)):
        assert get_sncf_theoretical_train_data() == fake_zip_bytes


def test_empty_theoretical_archive_error():
    """An empty download is rejected."""
    with (
        pytest.raises(EmptyRequiredFileError),
        patch("requests.get", return_value=mock_download(b"")),
    ):
        get_sncf_theoretical_train_data()


def test_corrupted_theoretical_archive_error():
    """A download that is not a zip archive is rejected."""
    with (
        pytest.raises(zipfile.BadZipFile),
        patch("requests.get", return_value=mock_download(b"<html>error</html>")),
    ):
        get_sncf_theoretical_train_data()


def test_find_missing_gtfs_files():
    """Absent and empty expected files are reported, extra files are ignored."""
    fake_zip_bytes = build_fake_zip(
        {
            "trips.txt": b"trip_id,route_id\n1,A",
            "routes.txt": b"",
            "agency.txt": b"agency_id\n1",
        }
    )

    assert find_missing_gtfs_files(
        fake_zip_bytes, ["trips.txt", "routes.txt", "stops.txt"]
    ) == ["routes.txt", "stops.txt"]


def test_empty_trip_updates_error():
    """Check if trip updates file is empty."""
    mock_response = MagicMock()
    mock_response.content = b""
    mock_response.raise_for_status = MagicMock()

    with (
        pytest.raises(EmptyRequiredFileError),
        patch("requests.get", return_value=mock_response),
    ):
        get_sncf_trip_update_train_data()
