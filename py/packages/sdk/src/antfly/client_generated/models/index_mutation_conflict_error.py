from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.index_mutation_conflict_error_error import IndexMutationConflictErrorError

T = TypeVar("T", bound="IndexMutationConflictError")


@_attrs_define
class IndexMutationConflictError:
    """An index mutation conflict. When `error` is `metadata_mutation_outcome_unknown`, the mutation may already have
    committed and callers must observe index state before deciding whether to issue another mutation.

        Attributes:
            error (IndexMutationConflictErrorError):
            message (str):
            retryable (bool):
    """

    error: IndexMutationConflictErrorError
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
        error = IndexMutationConflictErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        index_mutation_conflict_error = cls(
            error=error,
            message=message,
            retryable=retryable,
        )

        return index_mutation_conflict_error
