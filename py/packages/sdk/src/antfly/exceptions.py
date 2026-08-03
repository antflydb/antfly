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
