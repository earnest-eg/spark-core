from .PipelineBaseError import PipelineBaseError

class SchemaMismatchError(PipelineBaseError):
    """
    Raised when there is a mismatch between the expected schema and the actual schema of the data. 
        This could occur when the data source has changed, or when there is an issue with the data ingestion process that results in an unexpected schema.
    """
    pass