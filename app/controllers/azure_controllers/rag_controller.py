import logging
from app.services.azure_services.cog_search_service import AzureCogSearchService
from app.services.azure_services.openai_service import AzureOpenAIService

logger = logging.getLogger(__name__)

class RAGController:
    """
    A controller class to handle Retrieval-Augmented Generation (RAG) flows.
    """

    def __init__(self):
        """
        Initializes the RAGController with Azure Cognitive Search and OpenAI services.
        """
        self.search_service = AzureCogSearchService()
        self.openai_service = AzureOpenAIService(deployment="gpt-4o")
        logger.info("RAGController initialized.")

    def execute_rag_flow(self, user_query: str, top: int = 3, semantic_config: str = "test") -> str:
        """
        Executes the RAG flow: Retrieve relevant documents, construct context, and generate an answer.

        Args:
            user_query (str): The user's query.
            top (int): Number of top documents to retrieve. Defaults to 3.
            semantic_config (str): The semantic configuration name. Defaults to "test".

        Returns:
            str: The generated answer from OpenAI.
        """
        try:
            # Perform a semantic search with Azure Cognitive Search
            search_results = self.search_service.search_documents(
                search_text=user_query,
                query_type="semantic",
                semantic_configuration_name=semantic_config,
                query_caption="extractive",
                query_answer="extractive",
                query_answer_count=5,
                top=top
            )

            # Use a set to ensure no repeated paragraphs or answers
            unique_texts = set()
            for result in search_results:
                if "text" in result:
                    unique_texts.add(result["text"])

            # Combine unique texts into a single context
            context = "\n\n".join(unique_texts) if unique_texts else "No relevant information found."
            # Construct the prompt for OpenAI
            prompt = (
                f"Below is the user query:\n\n{user_query}\n\n"
                f"Below is the relevant context retrieved from the documents:\n\n{context}\n\n"
                "Based on the above context, please answer the user's query as accurately and comprehensively as possible."
            )

            # Query OpenAI
            response = self.openai_service.query(
                system_prompt="You are a helpful assistant that provides detailed and factual answers based on the provided context.",
                user_prompt=prompt
            )

            return response

        except Exception as e:
            logger.exception("An error occurred during the RAG flow.")
            return f"An error occurred: {str(e)}"