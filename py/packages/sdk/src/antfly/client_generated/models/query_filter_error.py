from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.query_filter_error_error import QueryFilterErrorError
from ..models.query_filter_error_field import QueryFilterErrorField
from ..models.query_filter_error_status import QueryFilterErrorStatus

T = TypeVar("T", bound="QueryFilterError")


@_attrs_define
class QueryFilterError:
    """A public filter or exclusion query contains an invalid or unsupported node.

    Attributes:
        status (QueryFilterErrorStatus):
        error (QueryFilterErrorError):
        message (str):
        field (QueryFilterErrorField):
        offending_node (str): Stable name of the first invalid or unsupported filter node.
        retryable (bool):
    """

    status: QueryFilterErrorStatus
    error: QueryFilterErrorError
    message: str
    field: QueryFilterErrorField
    offending_node: str
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        field = self.field.value

        offending_node = self.offending_node

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "field": field,
                "offending_node": offending_node,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = QueryFilterErrorStatus(d.pop("status"))

        error = QueryFilterErrorError(d.pop("error"))

        message = d.pop("message")

        field = QueryFilterErrorField(d.pop("field"))

        offending_node = d.pop("offending_node")

        retryable = d.pop("retryable")

        query_filter_error = cls(
            status=status,
            error=error,
            message=message,
            field=field,
            offending_node=offending_node,
            retryable=retryable,
        )

        return query_filter_error
