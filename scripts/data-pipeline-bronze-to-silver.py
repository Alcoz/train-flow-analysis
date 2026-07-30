import os
from dotenv import load_dotenv
from io import BytesIO

import polars as pl

from data_eng.utils.s3_connector import (
    connect_to_s3,
    send_object_to_s3,
    get_object_from_s3,
    get_folder_content_from_s3,
)

from data_eng.sncf_getter import (
    get_sncf_theoretical_train_data,
    get_sncf_trip_update_train_data,
)

from data_eng.sncf_transformer import (
    sncf_trip_updates_protobuf_to_sheets,
)

THEORY_DATA_FOLDER = "data/{layer}/theory/"
CONTINUE_DATA_FOLDER = "data/{layer}/continue/"

load_dotenv(".env")

s3_connector = connect_to_s3(
    endpoint_url=os.getenv("MINIO_API"),
    access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    secret_access_key=os.getenv("MINIO_SECRET_ACCESS_KEY"),
    region_name=os.getenv("REGION_NAME"),
)

s3_bucket_name = os.getenv("BUCKET_NAME")

###############################
#### Bronze layer
###############################

### Theoretical data
sncf_theoretical_data = get_sncf_theoretical_train_data()

for filename, file in sncf_theoretical_data["files"].items():
    send_object_to_s3(
        s3_client=s3_connector,
        bucket=s3_bucket_name,
        object=file,
        s3_filepath=THEORY_DATA_FOLDER.format(layer="bronze") + filename,
    )

### Continuous data
sncf_trip_update_data = get_sncf_trip_update_train_data()

send_object_to_s3(
    s3_client=s3_connector,
    bucket=s3_bucket_name,
    object=sncf_trip_update_data,
    s3_filepath=CONTINUE_DATA_FOLDER.format(layer="bronze") + "sncf_trip_update.pb",
)

###############################
#### Silver Layer
###############################

for filename in get_folder_content_from_s3(
    s3_client=s3_connector,
    bucket_name=s3_bucket_name,
    folder=THEORY_DATA_FOLDER.format(layer="bronze"),
):
    file = get_object_from_s3(
        s3_client=s3_connector, bucket=s3_bucket_name, filepath=filename
    )

    train_dataframe = pl.read_csv(
        file,
        separator=",",
        has_header=True,
        infer_schema_length=10_000,  # améliore l'inférence de types
        ignore_errors=True,  # utile si données légèrement sales
    )

    parquet_buffer = BytesIO()

    train_dataframe.write_parquet(parquet_buffer)

    parquet_buffer.seek(0)

    s3_connector.upload_fileobj(
        parquet_buffer,
        s3_bucket_name,
        THEORY_DATA_FOLDER.format(layer="silver")
        + filename.split("/")[-1].replace("txt", "parquet"),
    )

###

protobuf_sncf_data = get_object_from_s3(
    s3_client=s3_connector,
    bucket=s3_bucket_name,
    filepath=CONTINUE_DATA_FOLDER.format(layer="bronze") + "sncf_trip_update.pb",
)

trips_updates_df = sncf_trip_updates_protobuf_to_sheets(
    protobuf_sncf_data=protobuf_sncf_data
)

parquet_buffer = BytesIO()

trips_updates_df.write_parquet(parquet_buffer)

parquet_buffer.seek(0)

s3_connector.upload_fileobj(
    parquet_buffer,
    s3_bucket_name,
    CONTINUE_DATA_FOLDER.format(layer="silver") + "sncf_trip_update.parquet",
)
