import logging
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest, AnalyzeResult
from azure.core.polling import LROPoller
from app.config import Config

# Configure logging
logger = logging.getLogger(__name__)

class AzureDocIntelService:
    """
    A service class for interacting with Azure Document Intelligence to analyze documents.
    """

    def __init__(self, model_id: str = "prebuilt-layout") -> None:
        """
        Initializes the AzureDocIntelService with the required configurations.

        Args:
            model_id (str): The ID of the Azure Document Intelligence model to use. Defaults to 'prebuilt-layout'.
        """
        self.endpoint = Config.AZURE_DOC_INTEL_ENDPOINT
        self.api_key = Config.AZURE_DOC_INTEL_API_KEY
        self.model_id = model_id

        self.client = DocumentIntelligenceClient(
            endpoint=self.endpoint,
            credential=AzureKeyCredential(self.api_key)
        )
        logger.info(f"AzureDocIntelService initialized with model ID '{self.model_id}'.")

    def analyze_document_from_url(self, document_url: str) -> dict:
        """
        Analyzes a document from a URL using the specified model.

        Args:
            document_url (str): The URL of the document to analyze.

        Returns:
            LROPoller: A poller to track the analysis operation.
        """
        poller: LROPoller = self.client.begin_analyze_document(
            model_id=self.model_id,
            analyze_request=AnalyzeDocumentRequest(url_source=document_url)
        )
        logger.info(f"Started analysis for document at URL '{document_url}' with model ID '{self.model_id}'.")
        result: AnalyzeResult = poller.result()
        return result.as_dict()

    def analyze_document_from_binary(self, document_bytes: bytes) -> dict:
        """
        Analyzes a document from binary data using the specified model.

        Args:
            document_bytes (bytes): The binary content of the document to analyze.

        Returns:
            LROPoller: A poller to track the analysis operation.
        """
        poller: LROPoller = self.client.begin_analyze_document(
            model_id=self.model_id,
            analyze_request=AnalyzeDocumentRequest(bytes_source=document_bytes)
        )
        logger.info(f"Started analysis for binary document with model ID '{self.model_id}'.")
        result: AnalyzeResult = poller.result()
        return result.as_dict()