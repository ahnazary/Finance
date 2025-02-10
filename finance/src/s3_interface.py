"""Module to interact with s3"""

import os
from datetime import datetime

import boto3
from src.postgres_interface import PostgresInterface
from src.utils import emit_log

from config import BACKUP_TABLES, LOG_LEVEL


class S3Interface:
    """
    Class to interact with s3
    """

    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")

    def get_bucket_names(self) -> list:
        """
        Method to get the names of the buckets in s3

        Returns
        -------
        list
            list with the names of the buckets in s3
        """
        response = self.s3_client.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        return buckets

    def backup_tables(self, bucket_name: str = "ahnazary-finance-prod"):
        """
        Method to backup tables to s3
        """
        pg_interface = PostgresInterface()
        for table_name in BACKUP_TABLES:
            df = pg_interface.read_table_to_df(table=table_name)

            current_date = datetime.now().strftime("%Y-%m-%d")
            file_path = f"{os.getcwd()}/finance/src/data/{table_name}.parquet"

            # write df to parquet file in data directory
            df.to_parquet(file_path)

            # upload parquet file to s3
            self.s3_client.upload_file(
                Filename=file_path,
                Bucket=bucket_name,
                Key=f"backup/{table_name}/{current_date}.parquet",
            )

            emit_log(
                f"Uploaded {table_name} table to s3 bucket {bucket_name}",
                log_level=LOG_LEVEL,
            )
