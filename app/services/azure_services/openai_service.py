import logging
from openai import AzureOpenAI
from app.config import Config
from typing import List

# Configure logging
logger = logging.getLogger(__name__)

class AzureOpenAIService:
    """
    A service class to interact with Azure OpenAI for chat completions and image-based queries.
    """

    def __init__(self, deployment: str = "gpt-4o") -> None:
        """
        Initializes the AzureOpenAIService with deployment and credentials.

        Args:
            deployment (str): The Azure OpenAI deployment name. Defaults to 'gpt-4o'.
        """
        self.deployment = deployment
        self.client = AzureOpenAI(
            api_key=Config.AZURE_OPENAI_API_KEY,
            api_version=Config.AZURE_OPENAI_API_VERSION,
            azure_endpoint=Config.AZURE_OPENAI_ENDPOINT
        )
        self.messages: List[dict] = []  # Stores conversation history

    def clear_memory(self) -> None:
        """
        Clears the stored conversation history.
        """
        self.messages = []
        logger.info("Conversation memory cleared.")

    def add_user_message(self, text: str) -> None:
        """
        Adds a user message to the conversation memory.

        Args:
            text (str): The user input text to add to memory.
        """
        self.messages.append({"role": "user", "content": text})
        logger.info("User message added to conversation memory.")

    def query_json(self, prompt: str, use_memory: bool = True, response_format: str = "json_object") -> str:
        """
        Sends a query to Azure OpenAI and retrieves a JSON-formatted response.

        Args:
            prompt (str): The prompt to send to the model.
            use_memory (bool): Whether to include conversation history in the query. Defaults to True.
            response_format (str): The desired response format, defaults to 'json_object'.

        Returns:
            str: The model's JSON-formatted response.
        """
        messages = [{"role": "system", "content": "You are a helpful assistant designed to output JSON."}]
        if use_memory:
            messages += self.messages
        messages.append({"role": "user", "content": prompt})

        logger.info("Sending JSON query to Azure OpenAI.")
        completion = self.client.chat.completions.create(
            model=self.deployment,
            response_format={"type": response_format},
            messages=messages
        )
        response = completion.choices[0].message.content.strip()
        logger.info("JSON query successful.")
        return response

    def query(self, system_prompt: str, user_prompt: str) -> str:
        """
        Sends a system prompt and user prompt to Azure OpenAI for a response.

        Args:
            system_prompt (str): The system-level prompt for context.
            user_prompt (str): The user-level input prompt.

        Returns:
            str: The model's response.
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        logger.info("Sending query to Azure OpenAI.")
        completion = self.client.chat.completions.create(
            model=self.deployment,
            messages=messages
        )
        response = completion.choices[0].message.content.strip()
        logger.info("Query executed successfully.")
        return response

    def query_with_image_url(self, prompt: str, image_urls: List[str]) -> str:
        """
        Sends a query along with one or more image URLs to Azure OpenAI.

        Args:
            prompt (str): The text query to send.
            image_urls (List[str]): A list of image URLs to include in the query.

        Returns:
            str: The model's response.
        """
        messages = [{"role": "user", "content": [{"type": "text", "text": prompt}] +
                    [{"type": "image_url", "image_url": {"url": url}} for url in image_urls]}]

        logger.info("Sending query with image URLs to Azure OpenAI.")
        completion = self.client.chat.completions.create(
            model=self.deployment,
            messages=messages
        )
        response = completion.choices[0].message.content.strip()
        logger.info("Query with image URLs successful.")
        return response

    def query_json_with_image_url(self, prompt: str, image_urls: List[str], use_memory: bool = False) -> str:
        """
        Sends a JSON query along with one or more image URLs to Azure OpenAI.

        Args:
            prompt (str): The text query to send.
            image_urls (List[str]): A list of image URLs to include in the query.
            use_memory (bool): Whether to include conversation history in the query. Defaults to False.

        Returns:
            str: The model's JSON-formatted response.
        """
        if not isinstance(image_urls, list) or not all(isinstance(url, str) for url in image_urls):
            raise ValueError("image_urls must be a list of strings.")

        messages = [{"role": "system", "content": "You are a helpful assistant designed to output JSON."}]
        if use_memory:
            messages += self.messages

        messages.append({"role": "user", "content": [{"type": "text", "text": prompt}] +
                        [{"type": "image_url", "image_url": {"url": url}} for url in image_urls]})

        logger.info("Sending JSON query with image URLs to Azure OpenAI.")
        completion = self.client.chat.completions.create(
            model=self.deployment,
            messages=messages,
            response_format={"type": "json_object"}
        )
        response = completion.choices[0].message.content.strip()
        logger.info("JSON query with image URLs successful.")
        return response