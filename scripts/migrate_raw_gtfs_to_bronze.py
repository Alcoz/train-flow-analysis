"""Copy the theoretical GTFS archives from raw/ to the bronze layer.

The archives used to be stored under raw/{date}/sncf_gtfs.zip; the bronze
layer now holds them under data/bronze/theory/date={date}/sncf_gtfs.zip.
This one-off migration copies the history (server-side copy, raw/ is kept).

It is idempotent: archives already present in bronze are skipped.

Usage (S3 credentials read from the environment or a .env file):
    uv run scripts/migrate_raw_gtfs_to_bronze.py            # dry run
    uv run scripts/migrate_raw_gtfs_to_bronze.py --apply    # copy
"""

import argparse
import os
import re

from data_eng.utils.s3_connector import connect_to_s3
from dotenv import load_dotenv

RAW_ARCHIVE = re.compile(r"^raw/(\d{4}-\d{2}-\d{2})/sncf_gtfs\.zip$")
BRONZE_ARCHIVE = "data/bronze/theory/date={date}/sncf_gtfs.zip"


def list_keys(s3_client, bucket: str, prefix: str) -> set[str]:
    """Return every object key under a prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    return {
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    }


def main():
    """Copy the raw GTFS archives missing from the bronze layer."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--apply", action="store_true", help="copy the files (dry run otherwise)"
    )
    args = parser.parse_args()

    load_dotenv(".env")
    s3_client = connect_to_s3(
        endpoint_url=os.environ["S3_API"],
        access_key_id=os.environ["S3_ACCESS_KEY"],
        secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        region_name=os.environ["REGION_NAME"],
    )
    bucket = os.environ["BUCKET_NAME"]

    bronze_keys = list_keys(s3_client, bucket, "data/bronze/theory/")

    copied = skipped = 0
    for raw_key in sorted(list_keys(s3_client, bucket, "raw/")):
        match = RAW_ARCHIVE.match(raw_key)
        if not match:
            print(f"ignored   {raw_key}")
            continue

        bronze_key = BRONZE_ARCHIVE.format(date=match.group(1))
        if bronze_key in bronze_keys:
            print(f"present   {bronze_key}")
            skipped += 1
            continue

        print(f"{'copy' if args.apply else 'to copy'}   {raw_key} -> {bronze_key}")
        if args.apply:
            s3_client.copy_object(
                Bucket=bucket,
                Key=bronze_key,
                CopySource={"Bucket": bucket, "Key": raw_key},
            )
        copied += 1

    action = "copied" if args.apply else "to copy (dry run, use --apply)"
    print(f"\n{copied} archive(s) {action}, {skipped} already in bronze.")


if __name__ == "__main__":
    main()
