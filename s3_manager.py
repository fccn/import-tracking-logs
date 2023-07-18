import io
import requests
import boto3
from decouple import config
from file_utils import calculate_checksum, Uncompress
import os
import glob

class CustomS3Manager:
    def __init__(self, s3_client=None):
        if s3_client is None:
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=config("ACCESS_KEY"),
                aws_secret_access_key=config("SECRET_KEY"),
                endpoint_url=config("ENDPOINT_URL")
            )
        else:
            self.s3 = s3_client

        self.bucket_name = config("BUCKET_NAME")
        self.local_file_path = os.path.expanduser("~/")
        self.ralph_endpoint_url = config("RALPH_ENDPOINT_URL")
        self.show_prints = config("SHOW_PRINTS", default="True", cast=bool)

    def print_if_enabled(self, message):
        if self.show_prints:
            print(message)


    def list_files(self, prefix=""):
        response = self.s3.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=config("PATH_S3_TO_TRACKINGLOG") + prefix
        )

        files_content = response["Contents"]
        downloaded_files = []

        downloaded_files_path = "downloaded_files.txt"
        if os.path.exists(downloaded_files_path):
            with open(downloaded_files_path, "r") as file:
                downloaded_files = [line.strip() for line in file.readlines()]

        files_to_download = []
        for content in files_content:
            file_key = content["Key"]
            if file_key in downloaded_files:
                self.print_if_enabled(f"File {file_key} is already downloaded.")
                continue

            if file_key.endswith("/"):  # It's a subfolder
                subfolder = file_key[len(prefix):-1]  # Remove the prefix and trailing slash
                files_to_download += self.list_files(prefix + subfolder)

            else:  # It's a file
                self.print_if_enabled("Caminho: " + file_key)
                file_path = self.local_file_path + file_key.split("/")[-1]
                if os.path.exists(file_path):
                    self.print_if_enabled(f"File {file_key} is downloaded.")
                    downloaded_files.append(file_key)
                else:
                    files_to_download.append(content)

        return files_to_download


    def list_folders(self):
        response = self.s3.list_objects_v2(
            Bucket=self.bucket_name,
            Delimiter="/"
        )

        for folder in response.get("CommonPrefixes", []):
            self.print_if_enabled(f"Folder: {folder['Prefix']}")

    def download_files(self):
        data = self.list_files()
        total_files = len(data)
        downloaded_files = set()
        error_files = set()

        downloaded_files_path = "downloaded_files.txt"
        if os.path.exists(downloaded_files_path):
            try:
                with open(downloaded_files_path, "r") as file:
                    downloaded_files = {line.strip() for line in file.readlines()}
            except io.UnsupportedOperation:
                downloaded_files = set()

        error_files_path = "error_files.txt"
        if os.path.exists(error_files_path):
            try:
                with open(error_files_path, "r") as file:
                    error_files = {line.strip() for line in file.readlines()}
            except io.UnsupportedOperation:
                error_files = set()

        for i, file_data in enumerate(data, start=1):
            file_key = file_data["Key"]

            file_path = self.local_file_path + file_key.split("/")[-1]
            if file_key in downloaded_files:
                self.print_if_enabled(f"File {file_key} is already downloaded.")
                continue

            try:
                self.s3.download_file(
                    self.bucket_name,
                    file_key,
                    file_path
                )
                print(f"Downloaded {file_key} ({i}/{total_files})")

                Uncompress.extract_files(file_path)
                downloaded_files.add(file_key)

                self.send_file_to_endpoint(file_path)
                self.remove_files(file_path)

            except Exception as e:
                self.print_if_enabled(f"Error occurred while downloading {file_key}: {str(e)}")
                error_files.add(file_key)

            if downloaded_files:
                with open(downloaded_files_path, "w") as file:
                    existing_files = {line.strip() for line in file.readlines()} if file.readable() else set()
                    new_files = downloaded_files - existing_files
                    file.writelines(f"{file_key}\n" for file_key in new_files)
                self.print_if_enabled("Downloaded file keys appended to downloaded_files.txt")

            if error_files:
                with open(error_files_path, "w") as file:
                    existing_files = {line.strip() for line in file.readlines()} if file.readable() else set()
                    new_files = error_files - existing_files
                    file.writelines(f"{file_key}\n" for file_key in new_files)
                self.print_if_enabled("Error file keys appended to error_files.txt")

        if not data:
            self.print_if_enabled("No files available for download.")



    def send_file_to_endpoint(self, file_path):
        compressed_file_name = os.path.basename(file_path)
        json_file_name = compressed_file_name.rstrip(".gz")
        json_file_path = os.path.join(os.path.dirname(file_path), json_file_name)

        if os.path.exists(json_file_path):
            with open(json_file_path, "r") as json_file:
                json_data = json_file.read()
                response = requests.post(self.ralph_endpoint_url, data=json_data)
                if response.status_code == 200:
                    self.print_if_enabled(f"File {file_path} sent to endpoint successfully.")
                else:
                    self.print_if_enabled(f"Failed to send file {file_path} to endpoint.")
            os.remove(json_file_path)
            self.print_if_enabled(f"Removed file: {json_file_path}")
        else:
            self.print_if_enabled(f"File not found: {json_file_path}")

    def remove_files(self, file_path):
        directory = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)

        files_to_remove = glob.glob(os.path.join(directory, "*"))
        for file in files_to_remove:
            if os.path.isfile(file):
                os.remove(file)
                self.print_if_enabled(f"Removed file: {file}")

        if os.path.exists(file_path):
            os.remove(file_path)
            self.print_if_enabled(f"Removed file: {file_path}")
        else:
            self.print_if_enabled(f"Compressed file not found: {file_path}")



# Example Usage
s3_manager = CustomS3Manager()
s3_manager.download_files()
