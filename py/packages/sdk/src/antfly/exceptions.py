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


class IdempotentBatchError(AntflyException):
    """Typed durable outcome returned by an idempotent batch request."""

    def __init__(
        self,
        status_code: int,
        status: str,
        code: str,
        message: str,
        retryable: bool,
        transaction_id: str,
        reconcile: str,
    ) -> None:
        self.status_code = status_code
        self.status = status
        self.code = code
        self.detail = message
        self.retryable = retryable
        self.transaction_id = transaction_id
        self.reconcile = reconcile
        super().__init__(f"idempotent batch failed ({status_code}, {status}): {message}")


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


class StorageResourceExhaustedError(AntflyException):
    """Retryable storage admission rejection with an actionable delay."""

    def __init__(
        self,
        message: str,
        retry_after_ms: int,
        retry_after_seconds: int | None = None,
    ) -> None:
        self.status_code = 429
        self.code = "storage_resource_exhausted"
        self.detail = message
        self.retryable = True
        self.retry_after_ms = retry_after_ms
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"storage resource exhausted (429): {message}")


class IndexMutationTemporarilyUnavailableError(AntflyException):
    """Retryable index mutation admission or validation failure."""

    def __init__(self, code: str, message: str, retry_after_seconds: int | None = None) -> None:
        self.status_code = 503
        self.code = code
        self.detail = message
        self.retryable = True
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"index mutation temporarily unavailable (503): {message}")
