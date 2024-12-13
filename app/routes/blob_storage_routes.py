import logging
from flask import Blueprint, jsonify, request
from werkzeug.exceptions import BadRequest
from app.services.azure_services import AzureBlobStorageService  

# Set up logging
logger = logging.getLogger(__name__)

# Create a Flask blueprint
blob_storage_blueprint = Blueprint("blob_storage", __name__, url_prefix="/api/blob")

# Initialize Azure Blob Storage Service
azure_blob_service = AzureBlobStorageService()

@blob_storage_blueprint.route("/upload", methods=["POST"])
def upload_pdfs():
    """
    Endpoint to upload multiple PDF files to Azure Blob Storage.

    Expects:
        - 'files' in the request (multipart/form-data), which can contain multiple files.

    Returns:
        JSON response with upload statuses for each file.
    """
    try:
        # Check if 'files' is in the request
        if "files" not in request.files:
            raise BadRequest("No files part in the request.")

        files = request.files.getlist("files")

        # Validate files
        if not files or len(files) == 0:
            raise BadRequest("No files provided for upload.")

        upload_results = []
        for file in files:
            # Validate file type
            if not file or file.filename.split(".")[-1].lower() != "pdf":
                raise BadRequest(f"File '{file.filename}' must be a PDF.")

            # Use the original file name as the blob name
            blob_name = file.filename

            # Read file content
            file_content = file.read()

            # Upload to Azure Blob Storage
            upload_result = azure_blob_service.upload_to_blob_storage(
                blob_name=blob_name,
                data=file_content,
                content_type="application/pdf"
            )

            # Log upload success
            logger.info(f"File '{file.filename}' uploaded successfully as '{blob_name}'.")
            upload_results.append({
                "file_name": file.filename,
                "status": "uploaded",
                "details": upload_result
            })

        return jsonify({
            "message": "Files uploaded successfully.",
            "upload_results": upload_results
        }), 201

    except BadRequest as e:
        logger.error(f"Bad request: {str(e)}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Error during file upload: {str(e)}")
        return jsonify({"error": "An error occurred during file upload."}), 500


@blob_storage_blueprint.route("/list", methods=["GET"])
def list_blobs():
    """
    Endpoint to retrieve all blob names in the Azure Blob Storage container.

    Returns:
        JSON response containing a list of blob names.
    """
    try:
        # Retrieve the list of blob URLs
        blob_urls = azure_blob_service.list_blob_urls()

        # Extract blob names from URLs
        blob_names = [url.split("/")[-1] for url in blob_urls]

        logger.info(f"Retrieved {len(blob_names)} blobs from the container.")

        return jsonify({
            "message": "Blob names retrieved successfully.",
            "blobs": blob_names
        }), 200

    except Exception as e:
        logger.error(f"Error retrieving blob names: {str(e)}")
        return jsonify({"error": "An error occurred while retrieving blob names."}), 500