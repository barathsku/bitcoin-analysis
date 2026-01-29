"""
Structured exception hierarchy for pipeline error handling.
"""


class PipelineError(Exception):
    """Base exception for all pipeline errors."""
    pass


class RetryableError(PipelineError):
    """
    Errors that should trigger a retry.
    
    Examples:
    - Network timeouts
    - 5xx server errors
    - 429 rate limit errors
    - Temporary service unavailability
    """
    pass


class FatalError(PipelineError):
    """
    Errors that should NOT retry.
    
    Examples:
    - 4xx client errors (except 429)
    - Schema mismatches
    - Invalid configuration
    - Authentication failures
    """
    pass


class ValidationError(FatalError):
    """Data quality validation failures from Soda Core checks."""
    
    def __init__(self, message: str, failed_checks: list = None):
        super().__init__(message)
        self.failed_checks = failed_checks or []


class ExtractionError(RetryableError):
    """API extraction failures (network, timeout, server errors)."""
    pass


class ConfigurationError(FatalError):
    """Invalid configuration or contract errors."""
    pass
