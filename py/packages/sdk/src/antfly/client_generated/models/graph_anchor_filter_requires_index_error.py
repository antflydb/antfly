from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_anchor_filter_requires_index_error_error import GraphAnchorFilterRequiresIndexErrorError
from ..models.graph_anchor_filter_requires_index_error_status import GraphAnchorFilterRequiresIndexErrorStatus

T = TypeVar("T", bound="GraphAnchorFilterRequiresIndexError")


@_attrs_define
class GraphAnchorFilterRequiresIndexError:
    """
    Attributes:
        status (GraphAnchorFilterRequiresIndexErrorStatus):
        error (GraphAnchorFilterRequiresIndexErrorError):
        message (str):
        retryable (bool):
    """

    status: GraphAnchorFilterRequiresIndexErrorStatus
    error: GraphAnchorFilterRequiresIndexErrorError
    message: str
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = GraphAnchorFilterRequiresIndexErrorStatus(d.pop("status"))

        error = GraphAnchorFilterRequiresIndexErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        graph_anchor_filter_requires_index_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
        )

        return graph_anchor_filter_requires_index_error
