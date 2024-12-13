# Financal Data Scraper [![Run Tests](https://github.com/alberttangalbert/financial-data-scraper/actions/workflows/run-tests.yml/badge.svg)](https://github.com/alberttangalbert/financial-data-scraper/actions/workflows/run-tests.yml)

## Description
This tool uses Retrieval-Augmented Generation (RAG) to extract and process financial data from uploaded PDF documents. It currently supports the automated generation of income statements and, in the future, will include balance sheets, cash flow statements, and statements of stockholders’ equity. Users can upload PDFs, and the system extracts, aggregates, and organizes financial metrics into clear, structured formats. The platform integrates advanced AI and Azure services to deliver accurate and professional results efficiently.

## Steps to Run Locally
1. Create and populate .env file, example given
2. Export .env variables to environment
3. Install requirements
4. Run the Server
    ```
    $ gunicorn -b 0.0.0.0:5000 run:app
    or 
    $ python run.py
    or
    $ docker build -t rfp-azure-bridge .
    $ docker run --env-file .env -p 5001:5000 rfp-azure-bridge
    ```

## Methodology 
The tool leverages Azure Document Intelligence (DocIntel) to scan PDFs and extract structured data, including tables and text. Extracted tables are parsed and classified into relevant financial categories, such as income statements, with all tables and paragraphs stored in an Azure Cognitive Search index. Azure OpenAI, integrated with the search index, is utilized to perform Retrieval-Augmented Generation (RAG) for extracting and consolidating key financial insights. Income statements are generated through iterative LLM prompting, ensuring logical accuracy and reconciliation of all numerical values. The methodology was refined through collaborative sessions with analysts to align with professional standards and will extend to balance sheets, cash flow statements, and stockholders’ equity in future developments.

## Current Endpoints and Example Outputs

1. Upload PDFs
    - Uploads multiple PDF files to Azure Blob Storage.
    - Endpoint: 
        ```
        POST /api/blob/upload
        ```
    - Example Request (via cURL):
        ```
        $ curl -X POST -F "files=@file1.pdf" -F "files=@file2.pdf" http://127.0.0.1:5000/api/blob/upload
        ```
    - Example Response
        ```
        {
            "message": "Files uploaded successfully.",
            "upload_results": [
                {
                    "file_name": "file1.pdf",
                    "status": "uploaded",
                    "details": {
                        "container": "blob-container",
                        "blob_name": "file1.pdf",
                        "status": "uploaded"
                    }
                },
                {
                    "file_name": "file2.pdf",
                    "status": "uploaded",
                    "details": {
                        "container": "blob-container",
                        "blob_name": "file2.pdf",
                        "status": "uploaded"
                    }
                }
            ]
        }
2. List Blobs
    - Retrieves a list of all blob names from the Azure Blob Storage container.
    - Endpoint: 
        ```
        GET /api/blob/list
        ```
    - Example Request (via cURL):
        ```
        $ curl -X GET http://127.0.0.1:5000/api/blob/list
        ```
    - Example Response:
        ```
        {
            "message": "Blob names retrieved successfully.",
            "blobs": [
                "file1.pdf",
                "file2.pdf",
                "report2023.pdf"
            ]
        }
        ```
3. Process Documents
    - Processes a list of blob names and generates an income statement. The processed results are stored as an Excel file in Azure Blob Storage and a SAS URL is returned for downloading.
    - Endpoint: 
        ```
        POST /api/documents/process
        ```
    - Example Request (via cURL):
        ```
        $ curl -X POST -H "Content-Type: application/json" -d '{"blob_names": ["file1.pdf", "file2.pdf"]}' http://127.0.0.1:5000/api/documents/process
        ```
    - Example Response:
        ```
        {
            "sas_url": "https://storageaccount.blob.core.windows.net/container/income_statement.xlsx?SAS_TOKEN"
        }
        ```
4. RAG Query
    - Handles the Retrieval-Augmented Generation (RAG) flow by retrieving relevant documents and generating an answer using Azure OpenAI.
    - Endpoint:
        ```
        POST /api/chatbot/rag_query
        ```
    - Example Request (via cURL):
        ```
        $ curl -X POST -H "Content-Type: application/json" -d '{"query": "What is the gross profit for FY 2023?"}' http://127.0.0.1:5000/api/chatbot/rag_query
        ```
    - Example Response:
        ```
        {
            "answer": "The gross profit for FY 2023 is $4,749,599."
        }
        ```
            
## Tests 
    - To-run
        ```
        $ python -m unittest discover tests
        ```