from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_distinct_budget_exceeded_error_dimension import GraphDistinctBudgetExceededErrorDimension
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
        operation (str): Named graph operation whose exact distinct aggregation exhausted the request budget.
        dimension (GraphDistinctBudgetExceededErrorDimension): Distinct aggregation resource exhausted by the operation.
        maximum (int): Configured request ceiling for the exhausted resource.
        remediation (str): Stable user-facing guidance for reducing exact distinct state.
    """

    status: GraphDistinctBudgetExceededErrorStatus
    error: GraphDistinctBudgetExceededErrorError
    message: str
    retryable: bool
    operation: str
    dimension: GraphDistinctBudgetExceededErrorDimension
    maximum: int
    remediation: str

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        operation = self.operation

        dimension = self.dimension.value

        maximum = self.maximum

        remediation = self.remediation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "retryable": retryable,
                "operation": operation,
                "dimension": dimension,
                "maximum": maximum,
                "remediation": remediation,
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

        operation = d.pop("operation")

        dimension = GraphDistinctBudgetExceededErrorDimension(d.pop("dimension"))

        maximum = d.pop("maximum")

        remediation = d.pop("remediation")

        graph_distinct_budget_exceeded_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
            operation=operation,
            dimension=dimension,
            maximum=maximum,
            remediation=remediation,
        )

        return graph_distinct_budget_exceeded_error
