from google.cloud import storage
import json
import pandas as pd
import io

class GCSModule:
    def __init__(self, bucket_name, credentials_path):
        """
        Initialize Google Cloud Storage client
        credentials_path: Path to your Google Cloud service account key JSON file
        """
        self.storage_client = storage.Client.from_service_account_json(credentials_path)
        self.bucket_name = bucket_name
        self.bucket = self.storage_client.bucket(bucket_name)

    def upload_file_to_bucket(self, file_content, destination_blob_name):
        """Upload file content to Google Cloud Storage bucket"""
        blob = self.bucket.blob(destination_blob_name)
        
        if isinstance(file_content, str):
            blob.upload_from_string(file_content)
        else:
            blob.upload_from_file(file_content)
        
        print(f"File uploaded to {destination_blob_name}")