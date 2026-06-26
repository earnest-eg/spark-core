from errors.AgentBaseError import AgentBaseError


class AgentRateLimitError(AgentBaseError):
    """
    Custom exception for agent rate limit errors.

    Carries structured metadata so FailoverManager can decide whether to
    drop the key immediately (TPD) or sleep-and-retry (TPM / RPM).

    Attributes:
        limit_type (str): One of ``"tpd"`` (tokens per day),
            ``"tpm"`` (tokens per minute), or ``"rpm"`` (requests per minute).
        retry_after (float): Seconds the upstream API suggests waiting
            before retrying.  ``0.0`` when the value could not be parsed.
    """

    def __init__(
        self,
        message: str,
        limit_type: str = "rpm",
        retry_after: float = 0.0,
    ):
        super().__init__(message)
        self.limit_type: str = limit_type
        self.retry_after: float = retry_after