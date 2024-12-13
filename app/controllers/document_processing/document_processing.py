import os
os.chdir("/Users/atang/Documents/bpc-spg/financial-scraper-server")
from typing import List
from app.controllers.document_processing.utils import doc_intel_utils, general_utils, openai_utils, cog_search_utils
from app.services.azure_services.openai_service import AzureOpenAIService
from app.services.azure_services.blob_storage_service import AzureBlobStorageService
from app.controllers.azure_controllers.cog_search_controller import CogSearchController
from app.core.fs_generators.income_statement_gen import generate_income_statement

def process_documents(blob_names: List[str]) -> str:
    """
    Processes documents from Azure Blob Storage, extracts structured data,
    and uploads an aggregated income statement DataFrame to Azure Blob Storage.

    Args:
        blob_names (List[str]): List of blob names to process.

    Returns:
        str: The blob as a sas url of the uploaded Excel sheet.
    """
    openai_service = AzureOpenAIService()
    cog_search_controller = CogSearchController()
    blob_service = AzureBlobStorageService()
    results = {}

    for blob_name in blob_names:
        print(f"Processing document: {blob_name}")

        # Step 1: Analyze Document
        analyze_document_result = doc_intel_utils.process_blob_document(blob_name)

        # Step 2: Convert Analyze Document to Structured Data
        text, text_sources, table_indicator, table_sources = general_utils.convert_analyze_document_to_structured_data(
            result=analyze_document_result
        )

        # Step 3: Extract Metadata
        year_ended = openai_utils.extract_fiscal_year_end(text, openai_service)
        company_name = openai_utils.extract_company_name(text, openai_service)

        # Step 4: Upload results to Azure Cognitive Search
        cog_search_utils.process_and_upload_documents(
            text=text,
            text_sources=text_sources,
            table_indicator=table_indicator,
            blob_name=blob_name,
            year_ended=year_ended,
            company_name=company_name,
            cog_search_controller=cog_search_controller
        )

        # Step 5: Process Tables
        dfs = [t for i, t in enumerate(text) if table_indicator[i]]
        classifications = openai_utils.classify_multiple_tables(dfs=dfs)

        # Filter Tables by Classification
        income_statement_dfs = [
            df for df, classification in zip(dfs, classifications) if classification == "Income Statement"
        ]

        # Step 6: Extract Unit Scale
        unit_scale = openai_utils.extract_unit_scale("\n\n".join(income_statement_dfs), openai_service)

        # Step 7: Generate Income Statement
        dataframes, amounts = generate_income_statement(
            income_statement_dfs=income_statement_dfs,
            unit_scale=unit_scale,
            year_ended=year_ended
        )

        # Step 8: Add Results to Dictionary
        results[year_ended] = (dataframes, amounts)

    # Step 9: Aggregate Income Statements
    aggregated_table = openai_utils.aggregate_income_statements(results)

    df = general_utils.parse_table_from_response(aggregated_table)

    # Step 10: Store Aggregated DataFrame in Azure Blob Storage
    excel_blob_name = general_utils.store_dataframe_to_blob(df, blob_service)
    return blob_service.get_blob_sas_url(excel_blob_name)