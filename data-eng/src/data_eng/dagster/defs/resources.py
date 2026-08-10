import dagster as dg

from data_eng.utils.s3_connector import connect_to_s3


class S3_Resource(dg.ConfigurableResource):
    s3_api: str
    s3_access_key: str
    s3_secret_access_key: str
    region_name: str
    bucket_name: str

    def get_client(self):
        return connect_to_s3(
            endpoint_url=self.s3_api,
            access_key_id=self.s3_access_key,
            secret_access_key=self.s3_secret_access_key,
            region_name=self.region_name,
        )
