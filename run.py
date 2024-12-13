import logging
from app import create_app
from dotenv import load_dotenv

def main():
    load_dotenv()

    app = create_app()

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting Flask application")

    # Run the app
    try:
        app.run(host="0.0.0.0", port=5000, debug=app.config.get("DEBUG", False))
    except Exception as e:
        logger.error(f"Failed to start Flask application: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
