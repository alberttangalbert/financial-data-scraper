import logging
from app.services.azure_services.cog_search_service import AzureCogSearchService
from app.controllers.decorators import error_handler

logger = logging.getLogger(__name__)

class CogSearchController:
    """
    Controller class for managing Azure Cognitive Search operations.
    Acts as a bridge between request data and service logic.
    """

    def __init__(self):
        self.cog_search_service = AzureCogSearchService()

    @error_handler
    def search_documents(self, request_data: dict) -> dict:
        """
        Controller method to handle document search.

        Args:
            request_data (dict): A dictionary containing search parameters.

        Returns:
            dict: The formatted search results.
        """
        search_text = request_data.get("search_text")
        if not search_text:
            raise ValueError("search_text is required.")

        # Extract optional parameters
        search_params = {
            "highlight_fields": request_data.get("highlight_fields", "question,answer"),
            "highlight_pre_tag": request_data.get("highlight_pre_tag", "<mark>"),
            "highlight_post_tag": request_data.get("highlight_post_tag", "</mark>"),
            "query_type": request_data.get("query_type", "simple"),
            "search_mode": request_data.get("search_mode", "any"),
            "semantic_configuration_name": request_data.get("semantic_configuration_name", "default"),
            "select": request_data.get("select")
        }

        results = self.cog_search_service.search_documents(search_text) #, **search_params)
        
        # Ensure results are JSON serializable
        return {"results": [result.as_dict() if hasattr(result, "as_dict") else result for result in results]}

    @error_handler
    def add_documents(self, request_data: dict) -> dict:
        """
        Controller method to handle adding documents.

        Args:
            request_data (dict): A dictionary containing documents to add.

        Returns:
            dict: The response from the add operation.
        """
        documents = request_data.get("documents")
        if not documents or not isinstance(documents, list):
            raise ValueError("The 'documents' field must be a non-empty list.")

        result = self.cog_search_service.add_documents(documents)
        
        # Ensure result is JSON serializable
        return [res.as_dict() if hasattr(res, "as_dict") else res for res in result]

    @error_handler
    def merge_documents(self, request_data: dict) -> dict:
        """
        Controller method to handle merging documents.

        Args:
            request_data (dict): A dictionary containing documents to merge.

        Returns:
            dict: The response from the merge operation.
        """
        documents = request_data.get("documents")
        if not documents or not isinstance(documents, list):
            raise ValueError("The 'documents' field must be a non-empty list.")

        result = self.cog_search_service.merge_documents(documents)
        
        # Ensure result is JSON serializable
        return [res.as_dict() if hasattr(res, "as_dict") else res for res in result]

    @error_handler
    def delete_documents(self, request_data: dict) -> dict:
        """
        Controller method to handle deleting documents.

        Args:
            request_data (dict): A dictionary containing document IDs to delete.

        Returns:
            dict: The response from the delete operation.
        """
        document_ids = request_data.get("document_ids")
        if not document_ids or not isinstance(document_ids, list):
            raise ValueError("The 'document_ids' field must be a non-empty list of document IDs.")
        document_ids_dict = [{"id": doc_id} for doc_id in document_ids]
        result = self.cog_search_service.delete_documents(document_ids_dict)
        
        # Ensure result is JSON serializable
        return [res.as_dict() if hasattr(res, "as_dict") else res for res in result]
    
    @error_handler
    def get_max_id(self) -> int:
        """
        Controller method to get the maximum value of the "id" field in the search index.

        Returns:
            int: The maximum value of the "id" field.
        """
        try:
            # Retrieve all documents, selecting only the 'id' field
            all_docs = self.cog_search_service.search_documents(
                search_text="*",
                select='id'
            )
            
            # Extract all IDs and find the maximum
            all_ids = [int(doc['id']) for doc in all_docs]
            
            if all_ids:
                return max(all_ids)
            else:
                return 0
        except Exception as e:
            # Handle any exceptions that might occur
            print(f"Error in get_max_id: {str(e)}")
            return -1
