import dagster as dg
from dotenv import load_dotenv
import os
from datetime import datetime

from data_eng.utils.s3_connector import connect_to_s3, send_file_to_s3
from data_eng.sncf_getter import get_sncf_theoretical_train_data


@dg.asset
def sncf_theoretical_data(context: dg.AssetExecutionContext):
    load_dotenv()
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ACCESS_KEY")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_SECRET_ACCESS_KEY")

    date = datetime.today().strftime("%Y-%m-%d")

    context.log.info(f"Processing date {date}")

    sncf_theoretical_data = get_sncf_theoretical_train_data()

    ########################################################

    s3_client = connect_to_s3(
        endpoint_url="http://localhost:9000",
        access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        secret_access_key=os.getenv("MINIO_SECRET_ACCESS_KEY"),
        region_name="eu-west-1",
    )

    send_file_to_s3(
        s3_client,
        bucket="sncf-bucket",
        object=sncf_theoretical_data["zip_file"],
        s3_filepath=f"raw/gtfs_theoretical_zip/sncf_gtfs_{date}.zip",
    )

    for filename, file in sncf_theoretical_data["files"].items():
        send_file_to_s3(
            s3_client,
            bucket="sncf-bucket",
            object=file,
            s3_filepath=f"raw/gtfs_theoretical_data/{date}/{filename}",
        )
