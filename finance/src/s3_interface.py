"""Module to interact with s3"""

import boto3


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
