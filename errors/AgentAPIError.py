from errors.AgentBaseError import AgentBaseError

class AgentAPIError(AgentBaseError):
    """
    Exception raised when an agent API request fails.
    """
    def __init__(self, message: str, original_exception: Exception | None = None):
        super().__init__(message)
        self.original_exception = original_exception
