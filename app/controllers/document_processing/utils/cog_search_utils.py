import uuid
import time
import json
from app.controllers.azure_controllers.cog_search_controller import CogSearchController

def check_existing_blob(
        blob_name: str, 
        cog_search_controller: CogSearchController
    ) -> bool:
    """
    Checks if a blob with the specified `blob_name` already exists in the Azure Cognitive Search index.

    Args:
        blob_name (str): The name of the blob to check.
        cog_search_controller (CogSearchController): An instance of the CogSearchController.

    Returns:
        bool: True if any entries with the same `blob_name` exist, False otherwise.
    """
    try:
        # Perform a search for the blob_name in the index
        search_results = cog_search_controller.search_documents(
            {"search_text": f"blob_name:{blob_name}"}
        )

        # Extract the `blob_name` field from each result and check for matches
        for result in search_results["results"]:
            if result.get("blob_name") == blob_name:
                return True

        return False
    except Exception as e:
        print(f"Error checking existing blob: {str(e)}")
        return False
    
def process_and_upload_documents(
    text: list,
    text_sources: list,
    table_indicator: list,
    blob_name: str,
    company_name: str,
    year_ended: str,
    cog_search_controller: CogSearchController
) -> None:
    """
    Processes and uploads documents to Azure Cognitive Search using the CogSearchController,
    but skips the upload if the `blob_name` already exists in the index.

    Args:
        text (list): List of textual content for each document section.
        text_sources (list): List of bounding regions or sources for each text.
        table_indicator (list): Boolean list indicating whether a section is a table.
        blob_name (str): The name of the blob the documents belong to.
        company_name (str): The name of the company associated with the documents.
        year_ended (str): The fiscal year for the documents.
        cog_search_controller (CogSearchController): An instance of the CogSearchController.

    Returns:
        None
    """
    try:
        # Check if the blob already exists in the index
        if check_existing_blob(blob_name, cog_search_controller):
            print(f"Blob '{blob_name}' already exists in the index. No action taken.")
            return

        # Fetch the current max document ID
        max_id = cog_search_controller.get_max_id()

        # Generate unique document group and document IDs
        document_group_id = str(int(uuid.uuid4().hex[:8], 16) + int(time.time() * 1000))
        document_id = str(int(uuid.uuid4().hex[:8], 16) + int(time.time() * 1000))

        # Prepare documents for upload
        documents_to_upload = []
        for i, t in enumerate(text):
            document = {
                "id": str(int(max_id) + i + 1),
                "text": t,
                "document_group_id": document_group_id,
                "bounding_regions": json.dumps(text_sources[i]),
                "blob_name": blob_name,
                "is_table": str(table_indicator[i]),
                "document_id": document_id,
                "company_name": company_name,
                "fiscal_year": year_ended,
                "quarter": ""
            }
            documents_to_upload.append(document)

        # Upload documents to Azure Cognitive Search
        cog_search_controller.add_documents({"documents": documents_to_upload})
        print(f"Documents successfully uploaded for blob '{blob_name}'.")

    except Exception as e:
        print(f"An error occurred: {e}")