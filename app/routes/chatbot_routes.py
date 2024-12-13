import logging
from flask import Blueprint, jsonify, request
from werkzeug.exceptions import BadRequest
from app.controllers.azure_controllers.rag_controller import RAGController

# Set up logging
logger = logging.getLogger(__name__)

# Create a Flask blueprint
chatbot_blueprint = Blueprint("chatbot", __name__, url_prefix="/api/chatbot")

# Initialize the RAG Controller
rag_controller = RAGController()

@chatbot_blueprint.route("/rag_query", methods=["POST"])
def rag_query():
    """
    Endpoint to handle the RAG flow for the chatbot.

    Expects:
        - 'query' in the request JSON (string): The user's query.
        - Optional 'top' (int): The number of documents to retrieve.
        - Optional 'semantic_config' (string): The semantic configuration name.

    Returns:
        JSON response with the generated answer.
    """
    try:
        # Parse request JSON
        data = request.get_json()
        if not data or "query" not in data:
            raise BadRequest("Request must include a 'query' field in the JSON body.")

        user_query = data["query"]
        top = data.get("top", 5)  # Default to 3 documents
        semantic_config = data.get("semantic_config", "test")  # Default semantic config

        # Log incoming request
        logger.info(f"Received RAG query: {user_query}, top={top}, semantic_config={semantic_config}")

        # Execute the RAG flow using the controller
        response = rag_controller.execute_rag_flow(user_query, top=top, semantic_config=semantic_config)

        # Return the response
        return jsonify({"answer": response}), 200

    except BadRequest as e:
        logger.error(f"Bad request: {str(e)}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return jsonify({"error": "An unexpected error occurred.", "details": str(e)}), 500
