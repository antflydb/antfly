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
