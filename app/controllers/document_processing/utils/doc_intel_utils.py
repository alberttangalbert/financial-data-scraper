import pandas as pd 
from typing import List, Tuple
import logging
import os
import pickle

from app.services.azure_services import AzureBlobStorageService
from app.services.azure_services.doc_intel_service import AzureDocIntelService

def process_blob_document(blob_name: str, cache_dir: str = "./cache/") -> dict:
    """
    Processes a document from Azure Blob Storage using AzureDocIntelService, 
    with caching of results in a .pkl file.

    Args:
        blob_name (str): The name of the blob in Azure Blob Storage.
        cache_dir (str): Directory to store the cache files.

    Returns:
        dict: The result of the document analysis.
    """
    # Initialize services
    blob_service = AzureBlobStorageService()
    doc_intel_service = AzureDocIntelService()

    # Create the cache file path
    cache_file = os.path.join(cache_dir, f"{blob_name}.pkl")

    # Check if the cache file exists
    if os.path.exists(cache_file):
        logging.info(f"Cache file found: {cache_file}. Loading result from cache.")
        # Load the result from the cache file
        with open(cache_file, 'rb') as f:
            analyze_document_result = pickle.load(f)
    else:
        logging.info(f"Cache file not found. Processing the document: {blob_name}")
        # Retrieve the binary content of the blob
        try:
            file_content = blob_service.get_blob_content(blob_name)
        except Exception as e:
            logging.error(f"Error retrieving blob content for '{blob_name}': {e}")
            raise RuntimeError(f"Failed to retrieve blob content for '{blob_name}'") from e

        # Process the document using AzureDocIntelService
        try:
            analyze_document_result = doc_intel_service.analyze_document_from_binary(file_content)
        except Exception as e:
            logging.error(f"Error processing document '{blob_name}': {e}")
            raise RuntimeError(f"Failed to process document '{blob_name}'") from e

        # Save the result to the cache file
        os.makedirs(cache_dir, exist_ok=True)  # Ensure the cache directory exists
        with open(cache_file, 'wb') as f:
            pickle.dump(analyze_document_result, f)
        logging.info(f"Result cached to: {cache_file}")

    return analyze_document_result

def analyze_result_dict_to_df(table: dict)  -> Tuple[pd.DataFrame, List[List[dict]]]:
    """
    Converts tables from the begin_analyze_document result into a cleaned pandas DataFrame.
    
    Parameters
    ----------
    table : dict
        A dictionary representation of a DocumentTable created by Azure Document Intelligence.

    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame representation of the DocumentTable.
    list
        A 2D list containing metadata (bounding regions) for each cell.
    """
    
    # Extract row and column counts
    row_count = table.get('rowCount', 0)
    column_count = table.get('columnCount', 0)
    
    # Return an empty DataFrame if table has no rows or columns
    if row_count == 0 or column_count == 0:
        return pd.DataFrame(), []

    # Initialize DataFrame and source list for metadata
    df: pd.DataFrame = pd.DataFrame(
        data=[[''] * column_count for _ in range(row_count)],
        columns=[''] * column_count
    )
    sources = [
        [[] for _ in range(column_count)] 
        for _ in range(row_count)
    ]

    column_headers = [None] * column_count

    # Populate DataFrame and source list with cell data
    for cell in table.get("cells", []):
        row_idx = cell.get("rowIndex", -1)
        col_idx = cell.get("columnIndex", -1)
        if row_idx == -1 or col_idx == -1:
            continue
        
        # Set column headers if the cell is of kind 'columnHeader'
        if cell.get("kind", "") == 'columnHeader':
            column_headers[col_idx] = cell["content"].strip()
        elif cell.get("content", ""):
            df.iat[row_idx, col_idx] = cell["content"].strip()
            # Add bounding region metadata if available
            sources[row_idx][col_idx] = cell.get("boundingRegions", [{}])[0]

    # Set column headers if available
    if any(column_headers):
        df.columns = column_headers

    # Filter out empty rows and update source list
    non_empty_rows = [i for i, row in df.iterrows() if any(row)]
    if non_empty_rows:
        df = df.iloc[non_empty_rows].reset_index(drop=True)
        sources = [sources[i] for i in non_empty_rows]
    else:
        return pd.DataFrame(), []  # Return empty if no non-empty rows exist

    return df, sources

