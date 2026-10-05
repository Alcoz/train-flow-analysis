"""Interface to extract data from sncf endpoint."""

import io
import zipfile

import requests


class EmptyRequiredFileError(Exception):
    """Error class if required file is empty."""

    pass


# GTFS files used downstream: their absence is reported, not enforced.
EXPECTED_GTFS_FILES = ["trips.txt", "routes.txt", "stops.txt"]


def get_sncf_theoretical_train_data():
    """Getter of the theoretical data of sncf relative to trains, stations or trips descriptions.

    The archive is returned as downloaded: its files are extracted later, in
    the silver layer, so that every partition can be rebuilt from bronze.

    Returns:
        bytes: the GTFS zip archive.

    Raises:
        EmptyRequiredFileError: the archive is empty.
        zipfile.BadZipFile: the content is not a valid zip archive.

    """
    sncf_theoretical_train_url = "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip"
    sncf_theoretical_train_data_zip_bytes = requests.get(sncf_theoretical_train_url)
    sncf_theoretical_train_data_zip_bytes.raise_for_status()

    content = sncf_theoretical_train_data_zip_bytes.content
    if not content:
        raise EmptyRequiredFileError("The GTFS archive is empty")

    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        corrupted_file = archive.testzip()
        if corrupted_file is not None:
            raise zipfile.BadZipFile(
                f"Corrupted file in GTFS archive: {corrupted_file}"
            )

    return content


def find_missing_gtfs_files(zip_bytes: bytes, expected_files: list[str]) -> list[str]:
    """List the expected files that are absent or empty in a GTFS archive.

    Args:
        zip_bytes (bytes): the GTFS zip archive.
        expected_files (list[str]): file names that should be in the archive.

    Returns:
        list[str]: the expected files that are missing or empty, sorted.

    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        present = {info.filename for info in archive.infolist() if info.file_size > 0}

    return sorted(set(expected_files) - present)


def get_sncf_trip_update_train_data():
    """Getter of the trip updates of the sncf.

    Returns:
        (bytes | Any): trip updates SNCF data in GTFS-RT.

    """
    sncf_trip_update_train_url = (
        "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates"
    )
    sncf_trip_update_train_data = requests.get(sncf_trip_update_train_url)
    sncf_trip_update_train_data.raise_for_status()

    sncf_trip_update_content = sncf_trip_update_train_data.content

    if not sncf_trip_update_content:
        raise EmptyRequiredFileError

    return sncf_trip_update_train_data.content
