class PipelineBaseError(Exception):
    """
    Base class for all pipeline-related errors. All custom exceptions in the pipeline should inherit from this class.

    Attributes:
        step_name (str): The name of the pipeline step where the error occurred.
        message (str): A detailed error message describing the issue.
    """
    def __init__(self, step_name: str, message: str):
        self.step_name = step_name
        self.message = f"[{step_name}]: {message}"
        super().__init__(self.message)