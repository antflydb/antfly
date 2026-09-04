from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.reranker_candidate_limit_exceeded_error_error import RerankerCandidateLimitExceededErrorError
from ..models.reranker_candidate_limit_exceeded_error_status import RerankerCandidateLimitExceededErrorStatus
from ..models.reranker_provider import RerankerProvider

T = TypeVar("T", bound="RerankerCandidateLimitExceededError")


@_attrs_define
class RerankerCandidateLimitExceededError:
    """
    Attributes:
        status (RerankerCandidateLimitExceededErrorStatus):
        error (RerankerCandidateLimitExceededErrorError):
        message (str):
        provider (RerankerProvider): The reranking provider to use.
        maximum (int):
        retryable (bool):
    """

    status: RerankerCandidateLimitExceededErrorStatus
    error: RerankerCandidateLimitExceededErrorError
    message: str
    provider: RerankerProvider
    maximum: int
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        provider = self.provider.value

        maximum = self.maximum

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "provider": provider,
                "maximum": maximum,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = RerankerCandidateLimitExceededErrorStatus(d.pop("status"))

        error = RerankerCandidateLimitExceededErrorError(d.pop("error"))

        message = d.pop("message")

        provider = RerankerProvider(d.pop("provider"))

        maximum = d.pop("maximum")

        retryable = d.pop("retryable")

        reranker_candidate_limit_exceeded_error = cls(
            status=status,
            error=error,
            message=message,
            provider=provider,
            maximum=maximum,
            retryable=retryable,
        )

        return reranker_candidate_limit_exceeded_error
