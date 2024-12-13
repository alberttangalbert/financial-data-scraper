import logging
from functools import wraps
from azure.core.exceptions import (
    HttpResponseError,
    ServiceRequestError,
    ServiceResponseError,
    ClientAuthenticationError,
    ResourceNotFoundError
)
import jwt

logger = logging.getLogger(__name__)

def error_handler(func):
    """
    A decorator to handle errors in controller methods.
    Catches specific Azure errors, JWT errors, and general exceptions,
    logs them, and returns a standardized error response.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Execute the function
            return func(*args, **kwargs)
        
        # Handle validation errors (client-side issues)
        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return {"error": str(e)}, 400

        # Handle Azure authentication errors
        except ClientAuthenticationError as e:
            logger.error(f"Azure authentication error: {e}")
            return {"error": "Authentication failed with Azure services", "details": str(e)}, 401

        # Handle Azure resource not found errors
        except ResourceNotFoundError as e:
            logger.error(f"Azure resource not found: {e}")
            return {"error": "The requested resource was not found in Azure", "details": str(e)}, 404

        # Handle Azure-specific HTTP response errors (server-side issues)
        except HttpResponseError as e:
            logger.error(f"Azure Cognitive Search error: {e}")
            return {"error": "Azure Cognitive Search service error", "details": str(e)}, 500

        # Handle Azure request errors (network issues, invalid requests)
        except ServiceRequestError as e:
            logger.error(f"Azure service request error: {e}")
            return {"error": "A service request error occurred with Azure", "details": str(e)}, 502

        # Handle Azure response errors (unexpected server responses)
        except ServiceResponseError as e:
            logger.error(f"Azure service response error: {e}")
            return {"error": "A service response error occurred with Azure", "details": str(e)}, 502

        # Handle JWT expiration errors
        except jwt.ExpiredSignatureError as e:
            logger.warning(f"JWT token expired: {e}")
            return {"error": "JWT token has expired. Please log in again."}, 401

        # Handle invalid JWT errors
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid JWT token: {e}")
            return {"error": "Invalid JWT token. Access denied."}, 401

        # Catch-all for any other unexpected exceptions
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}", exc_info=True)
            return {"error": "An unexpected error occurred", "details": str(e)}, 500

    return wrapper