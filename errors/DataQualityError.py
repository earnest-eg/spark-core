from .PipelineBaseError import PipelineBaseError

class DataQualityError(PipelineBaseError):
    """
    Raised when a data quality issue is detected in the pipeline. 
        This could include problems such as missing values, invalid formats, or outliers that violate expected data constraints.
    """
    pass