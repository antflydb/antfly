from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_distinct_budget_exceeded_error_error import GraphDistinctBudgetExceededErrorError
from ..models.graph_distinct_budget_exceeded_error_status import GraphDistinctBudgetExceededErrorStatus

T = TypeVar("T", bound="GraphDistinctBudgetExceededError")


@_attrs_define
class GraphDistinctBudgetExceededError:
    """
    Attributes:
        status (GraphDistinctBudgetExceededErrorStatus):
        error (GraphDistinctBudgetExceededErrorError):
        message (str):
        retryable (bool):
        max_distinct_identities (int): Maximum distinct table-qualified identities retained by one request.
        max_distinct_state_bytes (int): Maximum distinct aggregation working-set bytes admitted for one request,
            including identity payloads, containers, and output state.
    """

    status: GraphDistinctBudgetExceededErrorStatus
    error: GraphDistinctBudgetExceededErrorError
    message: str
    retryable: bool
    max_distinct_identities: int
    max_distinct_state_bytes: int

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        max_distinct_identities = self.max_distinct_identities

        max_distinct_state_bytes = self.max_distinct_state_bytes

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "retryable": retryable,
                "max_distinct_identities": max_distinct_identities,
                "max_distinct_state_bytes": max_distinct_state_bytes,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = GraphDistinctBudgetExceededErrorStatus(d.pop("status"))

        error = GraphDistinctBudgetExceededErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        max_distinct_identities = d.pop("max_distinct_identities")

        max_distinct_state_bytes = d.pop("max_distinct_state_bytes")

        graph_distinct_budget_exceeded_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
            max_distinct_identities=max_distinct_identities,
            max_distinct_state_bytes=max_distinct_state_bytes,
        )

        return graph_distinct_budget_exceeded_error
