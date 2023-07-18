import os
import unittest
import boto3
from moto import mock_s3
from s3_manager import CustomS3Manager


class CustomS3ManagerTests(unittest.TestCase):
    @mock_s3
    def test_list_files(self):
        # Create a mock S3 bucket
        bucket_name = "nau-development-edxapp-log-sync"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)

        # Upload test files to the mock S3 bucket
        s3_client.upload_file("test_file1.gz", bucket_name, "trackinglog/file1.gz")
        s3_client.upload_file("test_file2.gz", bucket_name, "trackinglog/file2.gz")
        s3_client.upload_file("test_file3.gz", bucket_name, "trackinglog/file3.gz")

        # Create an instance of CustomS3Manager with the mock S3 client
        s3_manager = CustomS3Manager(s3_client=s3_client)

        # Test list_files
        files = s3_manager.list_files()
        self.assertEqual(len(files), 3)

    @mock_s3
    def test_list_folders(self):
        # Create a mock S3 bucket
        bucket_name = "nau-development-edxapp-log-sync"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)

        # Create an instance of CustomS3Manager with the mock S3 client
        s3_manager = CustomS3Manager(s3_client=s3_client)

        # Test list_folders
        s3_manager.list_folders()

    @mock_s3
    def test_download_files(self):
        # Create a mock S3 bucket
        bucket_name = "nau-development-edxapp-log-sync"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)

        # Upload test files to the mock S3 bucket
        s3_client.upload_file("test_file1.gz", bucket_name, "trackinglog/file1.gz")
        s3_client.upload_file("test_file2.gz", bucket_name, "trackinglog/file2.gz")
        s3_client.upload_file("test_file3.gz", bucket_name, "trackinglog/file3.gz")

        # Create an instance of CustomS3Manager with the mock S3 client
        s3_manager = CustomS3Manager(s3_client=s3_client)

        # Test download_files
        s3_manager.download_files()

        # Assert that the files are downloaded and converted
        self.assertTrue(os.path.exists("~/file1.json"))
        self.assertTrue(os.path.exists("~/file2.json"))
        self.assertTrue(os.path.exists("~/file3.json"))


if __name__ == '__main__':
    unittest.main()
