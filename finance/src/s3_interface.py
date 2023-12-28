"""Module to interact with s3"""

import os
from datetime import datetime

import boto3
from src.postgres_interface import PostgresInterface
from src.utils import custom_logger

from config import BACKUP_TABLES


class S3Interface:
    """
    Class to interact with s3
    """

    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.logger = custom_logger(logger_name="s3_interface")

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
        pf_interface = PostgresInterface()
        for table_name in BACKUP_TABLES:
            df = pf_interface.read_table_to_df(table=table_name)

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

            self.logger.info(f"Uploaded {table_name} to s3")
