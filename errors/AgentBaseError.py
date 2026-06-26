class AgentBaseError(Exception):
    """
    Base class for all agent-related errors.

    Attributes:
        message (str): A detailed error message describing the issue.
    """
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message