import logging
from azure.core.credentials import AzureKeyCredential
from typing import List, Dict, Any
from azure.search.documents import SearchClient
from azure.search.documents.models import IndexingResult
from app.config import Config

# Configure logging
logger = logging.getLogger(__name__)

class AzureCogSearchService:
    """
    A service class for managing interactions with Azure Cognitive Search.
    """
    def __init__(self) -> None:
        """
        Initializes the AzureCogSearchService with the required configurations and credentials.
        """
        self.index_name = Config.AZURE_SEARCH_INDEX
        self.endpoint = Config.AZURE_SEARCH_ENDPOINT
        self.api_key = Config.AZURE_SEARCH_API_KEY

        self.search_client = SearchClient(
            endpoint=self.endpoint,
            index_name=self.index_name,
            credential=AzureKeyCredential(self.api_key)
        )
        logger.info(f"Initialized AzureCogSearchService with index '{self.index_name}'.")

    def search_documents(self, search_text: str, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        Search for documents in the Azure Cognitive Search index.

        Args:
            search_text (str): The text to search for.
            **kwargs: Additional search parameters for fine-tuned queries.

        Returns:
            List[Dict[str, Any]]: A list of search results.
        """
        results = self.search_client.search(search_text, **kwargs)
        logger.info(f"Performed search for '{search_text}' in index '{self.index_name}'.")
        return [result for result in results]

    def add_documents(self, documents: List[Dict[str, Any]]) -> List[IndexingResult]:
        """
        Add documents to the Azure Cognitive Search index.

        Args:
            documents (List[Dict[str, Any]]): A list of documents to add.

        Returns:
            List[IndexingResult]: The response from the upload operation.
        """
        result = self.search_client.upload_documents(documents)
        logger.info(f"Uploaded {len(documents)} documents to index '{self.index_name}'.")
        return result

    def merge_documents(self, documents: List[Dict[str, Any]]) -> List[IndexingResult]:
        """
        Merge documents into the Azure Cognitive Search index.

        Args:
            documents (List[Dict[str, Any]]): A list of documents to merge.

        Returns:
            List[IndexingResult]: The response from the merge operation.
        """
        result = self.search_client.merge_documents(documents)
        logger.info(f"Merged {len(documents)} documents into index '{self.index_name}'.")
        return result

    def delete_documents(self, document_ids: List[str]) -> List[IndexingResult]:
        """
        Delete documents from the Azure Cognitive Search index.

        Args:
            document_ids (List[str]): A list of document IDs to delete.

        Returns:
            List[IndexingResult]: The response from the delete operation.
        """
        result = self.search_client.delete_documents(documents=document_ids)
        logger.info(f"Deleted {len(document_ids)} documents from index '{self.index_name}'.")
        return result