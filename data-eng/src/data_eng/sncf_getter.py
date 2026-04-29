import requests
import zipfile
import io


def get_sncf_theoretical_train_data():
    sncf_theoretical_train_url = "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip"
    sncf_theoretical_train_data_zip_bytes = requests.get(sncf_theoretical_train_url)

    extracted_files = {}

    with zipfile.ZipFile(
        io.BytesIO(sncf_theoretical_train_data_zip_bytes.content)
    ) as archive:
        for filename in archive.namelist():
            # Ignore les dossiers éventuels
            if not filename.endswith("/"):
                extracted_files[filename] = archive.read(filename)

    return {
        "zip_file": sncf_theoretical_train_data_zip_bytes.content,
        "files": extracted_files,
    }


def get_sncf_trip_update_train_data():
    sncf_trip_update_train_url = (
        "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates"
    )
    sncf_trip_update_train_data = requests.get(sncf_trip_update_train_url)
    return sncf_trip_update_train_data.content
