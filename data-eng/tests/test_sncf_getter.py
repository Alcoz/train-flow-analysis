# Check if everything ok, zip uncorupted, zip not empty, list of files is the good one and there is no empty file
import io
import zipfile
from unittest.mock import MagicMock, patch

import pytest
from data_eng.sncf_getter import (
    EmptyRequiredFileError,
    MissingRequiredFileError,
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


def test_requested_files_in_zip():
    """Check if function handle corrupted import file."""
    fake_zip_bytes = build_fake_zip(
        {
            "trips.txt": b"trip_id,route_id\n1,A",
            "routes.txt": b"route_id\n2,B",
        }
    )

    mock_response = MagicMock()
    mock_response.content = fake_zip_bytes
    mock_response.raise_for_status = MagicMock()  # ne lève rien

    with (
        patch("requests.get", return_value=mock_response),
    ):
        get_sncf_theoretical_train_data()


def test_requested_files_in_zip_error():
    """Check if function handle corrupted import file."""
    fake_zip_bytes = build_fake_zip(
        {
            "trips.txt": b"trip_id,route_id\n1,A",
            "chemin.txt": b"route_id\n2,B",
        }
    )

    mock_response = MagicMock()
    mock_response.content = fake_zip_bytes
    mock_response.raise_for_status = MagicMock()  # ne lève rien

    with (
        pytest.raises(MissingRequiredFileError),
        patch("requests.get", return_value=mock_response),
    ):
        get_sncf_theoretical_train_data()


def test_empty_requested_files_in_zip_error():
    """Check if one of requested files is not empty."""
    fake_zip_bytes = build_fake_zip(
        {
            "trips.txt": b"trip_id,route_id\n1,A",
            "routes.txt": b"",
        }
    )

    mock_response = MagicMock()
    mock_response.content = fake_zip_bytes
    mock_response.raise_for_status = MagicMock()

    with (
        pytest.raises(EmptyRequiredFileError),
        patch("requests.get", return_value=mock_response),
    ):
        get_sncf_theoretical_train_data()


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
