from agents.AgentInterface import AgentInterface

from errors.AgentAPIError import AgentAPIError
from logs.logger import get_logger
from utils.decorators import handle_agent_errors

logger = get_logger()


class LlamaFourClient(AgentInterface):
    """
    Client implementation for Llama 4 Scout (17b-16e-instruct).

    This client inherits from AgentInterface and integrates with the 
    global error handling decorator to manage API-related exceptions 
    automatically during text generation.

    Attributes:
        name (str): Hardcoded as "Llama 4".
        model (str): Specific model path: "meta-llama/llama-4-scout-17b-16e-instruct".
    """

    def __init__(self, api_key: str | None = None):
        """
        Initializes the Llama 4 Client with the specific Groq model name.

        Args:
            api_key (str, optional): Groq API key. If None, attempts to inherit 
                from environment variables managed by the FailoverManager.
        """
        super().__init__(
            name="Llama 4",
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            api_key=api_key,
        )

    @handle_agent_errors(AgentAPIError)
    def generate_text(self, prompt: str) -> str:
        """
        Generates text using the Llama 4 model.

        This method wraps the parent class's implementation, ensuring that any 
        API errors (e.g., network issues, rate limits) are caught and handled 
        by the decorator.

        Args:
            prompt (str): The input prompt to send to the model.

        Returns:
            str: The generated text response.
        """
        return super().generate_text(prompt)