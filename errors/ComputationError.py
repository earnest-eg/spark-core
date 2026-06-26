from .PipelineBaseError import PipelineBaseError

class ComputationError(PipelineBaseError):
    """
    Raised when a computation error occurs in the pipeline. 
        This could be due to issues such as division by zero, overflow, or other runtime errors during data processing.
    """
    pass