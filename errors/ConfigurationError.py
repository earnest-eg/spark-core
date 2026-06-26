class ConfigurationError(Exception):
    """
    Raised when there is a configuration issue in the pipeline.
        This could include missing or invalid configuration parameters, issues with environment variables, or other problems related to the setup and configuration of the pipeline.
    """
    pass