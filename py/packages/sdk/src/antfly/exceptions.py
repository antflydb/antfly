"""Exception classes for Antfly SDK."""


class AntflyException(Exception):
    """Base exception for Antfly SDK."""

    pass


class AntflyConnectionError(AntflyException):
    """Raised when connection to Antfly server fails."""

    pass


class AntflyAuthError(AntflyException):
    """Raised when authentication fails."""

    pass


class InferenceAPIError(AntflyException):
    """Structured error returned by the inference API."""

    def __init__(
        self,
        status_code: int,
        code: str | None,
        message: str,
        retryable: bool | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.detail = message
        self.retryable = retryable
        super().__init__(f"inference request failed ({status_code}): {message}")


class InferenceCapacityError(InferenceAPIError):
    """Temporary inference-capacity rejection with an actionable retry delay."""

    def __init__(
        self,
        code: str,
        message: str,
        reason: str,
        retry_after_ms: int,
    ) -> None:
        self.reason = reason
        self.retry_after_ms = retry_after_ms
        super().__init__(503, code, message, True)
