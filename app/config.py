import os

class Config:
    """Configuration settings for Azure services, Adobe API, JWT secrets, and application settings."""
    
    # Azure Cognitive Search Configuration
    AZURE_SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
    AZURE_SEARCH_API_KEY = os.getenv("AZURE_SEARCH_API_KEY")
    AZURE_SEARCH_INDEX = os.getenv("AZURE_SEARCH_INDEX")
    
    # Azure Document Intelligence (Form Recognizer) Configuration
    AZURE_DOC_INTEL_ENDPOINT = os.getenv("AZURE_DOC_INTEL_ENDPOINT")
    AZURE_DOC_INTEL_API_KEY = os.getenv("AZURE_DOC_INTEL_API_KEY")

    # Azure MSAL (Entra ID) Configuration for frontend token validation
    AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
    AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
    AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
    AZURE_SCOPE = os.getenv("AZURE_SCOPE", "https://graph.microsoft.com/.default")

    # Azure OpenAI Configuration
    AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
    AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
    AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-01-preview")
    AZURE_OPENAI_DEPLOYMENT_ID = os.getenv("AZURE_OPENAI_DEPLOYMENT_ID")

    # Azure Blob Storage Configuration
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
    AZURE_STORAGE_CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

    # JWT Configuration for backend-generated safe keys
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
    JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
    
    # Backend audience for JWT validation
    BACKEND_AUDIENCE = os.getenv("BACKEND_AUDIENCE")

    # Token Cache File Path
    TOKEN_CACHE_FILE = os.getenv("TOKEN_CACHE_FILE", "./token_cache.bin")

    # Allowed origin for CORS policy
    ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN")

    @classmethod
    def validate(cls):
        """Ensures all required configuration values are set and raises an error if any are missing."""
        
        # Required configurations and descriptions
        required_configs = {
            "AZURE_SEARCH_ENDPOINT": "Azure Cognitive Search endpoint",
            "AZURE_SEARCH_API_KEY": "Azure Cognitive Search API key",
            "AZURE_SEARCH_INDEX": "Azure Cognitive Search index",
            "AZURE_DOC_INTEL_ENDPOINT": "Azure Document Intelligence endpoint",
            "AZURE_DOC_INTEL_API_KEY": "Azure Document Intelligence API key",
            "AZURE_CLIENT_ID": "Azure MSAL Client ID",
            "AZURE_CLIENT_SECRET": "Azure MSAL Client Secret",
            "AZURE_TENANT_ID": "Azure MSAL Tenant ID",
            "AZURE_OPENAI_ENDPOINT": "Azure OpenAI endpoint",
            "AZURE_OPENAI_API_KEY": "Azure OpenAI API key",
            "AZURE_OPENAI_API_VERSION": "Azure OpenAI API version",
            "AZURE_OPENAI_DEPLOYMENT_ID": "Azure OpenAI Deployment ID",
            "AZURE_STORAGE_CONNECTION_STRING": "Azure Blob Storage connection string",
            "AZURE_STORAGE_KEY": "Azure Blob Storage key for SAS generation",
            "AZURE_STORAGE_CONTAINER_NAME": "Azure Blob Storage container name",
            "JWT_SECRET_KEY": "JWT secret key for signing tokens",
            "BACKEND_AUDIENCE": "Backend audience for JWT token validation",
            "ALLOWED_ORIGIN": "Allowed origin for CORS policy"
        }

        # Check for missing configurations
        missing_configs = [name for name, description in required_configs.items() if not getattr(cls, name)]
        
        if missing_configs:
            raise EnvironmentError(
                f"The following environment variables are missing: {', '.join(missing_configs)}. "
                "Please check your environment variables and set them accordingly."
            )