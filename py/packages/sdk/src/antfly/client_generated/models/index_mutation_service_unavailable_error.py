from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.index_mutation_service_unavailable_error_error import IndexMutationServiceUnavailableErrorError

T = TypeVar("T", bound="IndexMutationServiceUnavailableError")


@_attrs_define
class IndexMutationServiceUnavailableError:
    """A retryable index mutation failure, including a distributed artifact-source protocol fence or a temporarily
    unavailable model probe.

        Attributes:
            error (IndexMutationServiceUnavailableErrorError):
            message (str):
            retryable (bool):
    """

    error: IndexMutationServiceUnavailableErrorError
    message: str
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        error = self.error.value

        message = self.message

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "error": error,
                "message": message,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        error = IndexMutationServiceUnavailableErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        index_mutation_service_unavailable_error = cls(
            error=error,
            message=message,
            retryable=retryable,
        )

        return index_mutation_service_unavailable_error
