from flask import Flask
from flask_cors import CORS  # Import CORS

from app.config import Config
from app.routes.blob_storage_routes import blob_storage_blueprint  # Import the blueprint
from app.routes.document_processing_routes import document_processing_blueprint  # Import the blueprint
from app.routes.chatbot_routes import chatbot_blueprint  # Import the blueprint
import logging

def create_app():
    """Factory function to create and configure the Flask app."""
    app = Flask(__name__)

    # Load configuration
    app.config.from_object(Config)

    # Validate configuration
    Config.validate()

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("Application started")

    # Configure CORS
    CORS(app, resources={r"/*": {"origins": Config.ALLOWED_ORIGIN}})
    logger.info(f"CORS allowed origin: {Config.ALLOWED_ORIGIN}")

    # Register blueprints
    app.register_blueprint(blob_storage_blueprint)
    app.register_blueprint(document_processing_blueprint)
    app.register_blueprint(chatbot_blueprint)

    return app