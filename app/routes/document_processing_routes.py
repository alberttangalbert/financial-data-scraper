import logging
from flask import Blueprint, jsonify, request
from werkzeug.exceptions import BadRequest
from typing import List
from app.controllers.document_processing.document_processing import process_documents

# Set up logging
logger = logging.getLogger(__name__)

# Create a Flask blueprint
document_processing_blueprint = Blueprint("document_processing", __name__, url_prefix="/api/documents")

@document_processing_blueprint.route("/process", methods=["POST"])
def process_blobs():
    """
    Endpoint to process a list of blob names and return the SAS URL of the processed data.

    Expects:
        - JSON body with a 'blob_names' key containing a list of blob names.

    Returns:
        JSON response with the SAS URL of the processed file.
    """
    try:
        # Parse request JSON
        data = request.get_json()
        if not data or "blob_names" not in data:
            raise BadRequest("Missing required 'blob_names' key in the request body.")

        blob_names: List[str] = data["blob_names"]
        if not isinstance(blob_names, list) or not all(isinstance(name, str) for name in blob_names):
            raise BadRequest("'blob_names' must be a list of strings.")

        # Log received blob names
        logger.info(f"Received blob names for processing: {blob_names}")

        # Process documents and get the SAS URL
        sas_url = process_documents(blob_names)

        # Return the SAS URL in the response
        return jsonify({"sas_url": sas_url}), 200

    except BadRequest as e:
        logger.error(f"BadRequest: {e}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.exception("An error occurred while processing documents.")
        return jsonify({"error": "An internal server error occurred.", "details": str(e)}), 500
