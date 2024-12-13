import logging
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
    BlobSasPermissions
)
from app.config import Config
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Union

logger = logging.getLogger(__name__)

class AzureBlobStorageService:
    """
    A service class for managing Azure Blob Storage operations, including uploading, downloading, 
    listing, deleting blobs, and generating SAS URLs.
    """
    def __init__(self) -> None:
        """
        Initializes the AzureStorageService with configurations for the connection string and container name.
        """
        self.connection_string = Config.AZURE_STORAGE_CONNECTION_STRING
        self.container_name = Config.AZURE_STORAGE_CONTAINER_NAME
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.account_key = Config.AZURE_STORAGE_KEY

    def list_blob_urls(self, file_type: str = '') -> List[str]:
        """
        Lists URLs of blobs in the container, optionally filtered by file type.

        Args:
            file_type (str): File extension filter (e.g., 'pdf'). Lists all files if empty.

        Returns:
            List[str]: List of URLs for the matching blobs.
        """
        container_client = self.blob_service_client.get_container_client(self.container_name)
        account_name = self.blob_service_client.account_name
        blob_urls = [
            f"https://{account_name}.blob.core.windows.net/{self.container_name}/{blob.name}"
            for blob in container_client.list_blobs()
            if not file_type or blob.name.endswith(file_type)
        ]
        logger.info(f"Listed {len(blob_urls)} blobs in container '{self.container_name}' with file type '{file_type}'.")
        return blob_urls

    def upload_to_blob_storage(self, blob_name: str, data: bytes, content_type: str = "application/octet-stream") -> Dict[str, Union[str, int]]:
        """
        Uploads a single blob to Azure Blob Storage.

        Args:
            blob_name (str): Name of the blob to upload.
            data (bytes): Data to upload.
            content_type (str): MIME type of the blob. Defaults to 'application/octet-stream'.

        Returns:
            Dict[str, Union[str, int]]: Information about the upload operation.
        """
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
            max_concurrency=5,
            timeout=300
        )
        logger.info(f"Successfully uploaded blob '{blob_name}' to container '{self.container_name}'.")
        return {"container": self.container_name, "blob_name": blob_name, "status": "uploaded"}

    def upload_multiple_blobs_to_storage(
        self,
        blobs_data: Dict[str, bytes],
        content_type: str = "application/octet-stream",
        include_sas_url: bool = False
    ) -> List[Dict[str, Union[str, int]]]:
        """
        Uploads multiple blobs to Azure Blob Storage.

        Args:
            blobs_data (Dict[str, bytes]): Dictionary with blob names as keys and data as values.
            content_type (str): MIME type for all blobs. Defaults to 'application/octet-stream'.
            include_sas_url (bool): If True, includes SAS URL for each uploaded blob in the results.

        Returns:
            List[Dict[str, Union[str, int]]]: List of upload statuses for each blob.
        """
        results = []
        for blob_name, data in blobs_data.items():
            result = self.upload_to_blob_storage(blob_name, data, content_type)
            if include_sas_url:
                result["blob_sas_url"] = self.get_blob_sas_url(blob_name)
            results.append(result)
        return results

    def delete_blob(self, blob_name: str) -> Dict[str, Union[str, int]]:
        """
        Deletes a specified blob from Azure Blob Storage.

        Args:
            blob_name (str): Name of the blob to delete.

        Returns:
            Dict[str, Union[str, int]]: Information about the deletion operation.
        """
        container_client = self.blob_service_client.get_container_client(self.container_name)
        container_client.delete_blob(blob_name)
        logger.info(f"Deleted blob '{blob_name}' from container '{self.container_name}'.")
        return {"container": self.container_name, "blob_name": blob_name, "status": "deleted"}

    def get_blob_content(self, blob_name: str) -> bytes:
        """
        Retrieves the content of a specified blob.

        Args:
            blob_name (str): Name of the blob to retrieve.

        Returns:
            bytes: The content of the blob.
        """
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        blob_data = blob_client.download_blob().readall()
        logger.info(f"Retrieved content of blob '{blob_name}' from container '{self.container_name}'.")
        return blob_data

    def get_blob_sas_url(self, blob_name: str) -> str:
        """
        Generates a SAS URL for a given blob.

        Args:
            blob_name (str): Name of the blob.

        Returns:
            str: SAS URL for the blob.
        """
        sas_token = generate_blob_sas(
            account_name=self.blob_service_client.account_name,
            container_name=self.container_name,
            blob_name=blob_name,
            account_key=self.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=24)
        )
        sas_url = f"https://{self.blob_service_client.account_name}.blob.core.windows.net/{self.container_name}/{blob_name}?{sas_token}"
        logger.info(f"Generated SAS URL for blob '{blob_name}'.")
        return sas_url