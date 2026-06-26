from abc import ABC, abstractmethod
from typing import Optional
from groq import Groq
from config.dotenv_config import config

class AgentInterface(ABC):
    """
    An abstract base class providing a standardized interface for interacting with 
    Groq Cloud LLM services. 
    
    This class handles the initialization of the Groq client and enforces a 
    consistent configuration for all derived AI agents.

    Attributes:
        name (str): A unique identifier for the agent instance.
        model (str): The specific Groq-supported model ID to use (e.g., 'llama3-8b-8192').
        api_key (str): The authentication key for the Groq API.
        client (Groq): The initialized Groq API client instance.
    """

    def __init__(self, name: str, model: str, api_key: Optional[str] = None):
        """
        Initializes the AgentInterface with necessary configuration.

        Args:
            name (str): Identifier for this agent.
            model (str): Model ID to be used for inference.
            api_key (str, optional): Groq API key. Defaults to loading from config.GROQ_API_KEY.

        Raises:
            ValueError: If no API key is provided and the environment variable is not set.
        """
        self.name: str = name
        self.model: str = model
        self.api_key: str = api_key or config.GROQ_API_KEY
        
        if not self.api_key:
            raise ValueError(
                f"[{name}] No API key provided and GROQ_API_KEY env var is not set. "
                "Pass an explicit api_key or set GROQ_API_KEY in your .env file."
            )
        self.client = Groq(api_key=self.api_key)

    @abstractmethod
    def generate_text(self, prompt: str) -> str:
        """
        Generates text based on the provided prompt using the configured Groq model.

        This method enforces a deterministic output (temperature=0.0) and 
        expects a JSON-formatted response for structured data extraction.

        Args:
            prompt (str): The input text or query to send to the LLM.

        Returns:
            str: The raw, stripped, and lowercase response from the model.

        Note:
            Subclasses should override this method if they need specific prompt 
            engineering or message handling.
        """
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful data assistant. Return only the extracted or predicted value without explanations or any additional text or thinking.",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )

        return response.choices[0].message.content.strip().lower()