"""Interface to extract data from sncf endpoint."""

import io
import zipfile

import requests


class MissingRequiredFileError(Exception):
    """Error class for missing required file in zip."""

    pass


class EmptyRequiredFileError(Exception):
    """Error class if required file is empty."""

    pass


def get_sncf_theoretical_train_data():
    """Getter of the theoretical data of sncf relative to trains, stations or trips descriptions.

    Returns:
        dict: dictionary containing the zipfile and zipfile content extracted. Files described with gtfs format.

    """
    sncf_theoretical_train_url = "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip"
    sncf_theoretical_train_data_zip_bytes = requests.get(sncf_theoretical_train_url)
    sncf_theoretical_train_data_zip_bytes.raise_for_status()

    requested_files = ["trips.txt", "routes.txt"]

    extracted_files = {}

    with zipfile.ZipFile(
        io.BytesIO(sncf_theoretical_train_data_zip_bytes.content)
    ) as archive:
        if set(requested_files).issubset(set(archive.namelist())):
            for filename in archive.namelist():
                # Ignore les dossiers éventuels
                if not filename.endswith("/") and filename in requested_files:
                    content = archive.read(filename)
                    if not content:
                        raise EmptyRequiredFileError(f"Le fichier {filename} est vide")

                    extracted_files[filename] = content
        else:
            missing = set(requested_files) - set(archive.namelist())
            raise MissingRequiredFileError(f"Missing files in zip: {missing}")
    return {
        "zip_file": sncf_theoretical_train_data_zip_bytes.content,
        "files": extracted_files,
    }


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
