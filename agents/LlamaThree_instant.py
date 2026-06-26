from agents.AgentInterface import AgentInterface

from errors.AgentAPIError import AgentAPIError
from logs.logger import get_logger
from utils.decorators import handle_agent_errors

logger = get_logger()


class LlamaThree_instantClient(AgentInterface):
    """
    Client implementation for Llama 3.1 Instant (8b).

    This client inherits from AgentInterface and integrates with the 
    global error handling decorator to manage API-related exceptions 
    automatically during text generation.

    Attributes:
        name (str): Hardcoded as "Llama 3.1 Instant".
        model (str): Specific model path: "llama-3.1-8b-instant".
    """

    def __init__(self, api_key: str | None = None):
        """
        Initializes the Llama 3.1 Instant Client with the specific Groq model name.

        Args:
            api_key (str, optional): Groq API key. If None, attempts to inherit 
                from environment variables managed by the FailoverManager.
        """
        super().__init__(
            name="Llama 3.1 Instant", model="llama-3.1-8b-instant", api_key=api_key
        )

    @handle_agent_errors(AgentAPIError)
    def generate_text(self, prompt: str) -> str:
        """
        Generates text using the Llama 3.1 Instant model.

        This method wraps the parent class's implementation, ensuring that any 
        API errors (e.g., network issues, rate limits) are caught and handled 
        by the decorator.

        Args:
            prompt (str): The input prompt to send to the model.

        Returns:
            str: The generated text response.
        """
        return super().generate_text(prompt)
