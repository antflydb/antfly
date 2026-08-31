from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_work_budget_exceeded_error_dimension import GraphWorkBudgetExceededErrorDimension
from ..models.graph_work_budget_exceeded_error_error import GraphWorkBudgetExceededErrorError
from ..models.graph_work_budget_exceeded_error_status import GraphWorkBudgetExceededErrorStatus

T = TypeVar("T", bound="GraphWorkBudgetExceededError")


@_attrs_define
class GraphWorkBudgetExceededError:
    """
    Attributes:
        status (GraphWorkBudgetExceededErrorStatus):
        error (GraphWorkBudgetExceededErrorError):
        message (str):
        retryable (bool):
        operation (str): Named graph operation whose exact execution exhausted the request budget.
        mode (str): Graph operation mode, such as match or pattern.
        dimension (GraphWorkBudgetExceededErrorDimension): Bounded resource exhausted by the operation.
        maximum (int): Configured request ceiling for the exhausted resource.
        remediation (str): Stable user-facing guidance for reducing graph work.
    """

    status: GraphWorkBudgetExceededErrorStatus
    error: GraphWorkBudgetExceededErrorError
    message: str
    retryable: bool
    operation: str
    mode: str
    dimension: GraphWorkBudgetExceededErrorDimension
    maximum: int
    remediation: str

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        operation = self.operation

        mode = self.mode

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
                "mode": mode,
                "dimension": dimension,
                "maximum": maximum,
                "remediation": remediation,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = GraphWorkBudgetExceededErrorStatus(d.pop("status"))

        error = GraphWorkBudgetExceededErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        operation = d.pop("operation")

        mode = d.pop("mode")

        dimension = GraphWorkBudgetExceededErrorDimension(d.pop("dimension"))

        maximum = d.pop("maximum")

        remediation = d.pop("remediation")

        graph_work_budget_exceeded_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
            operation=operation,
            mode=mode,
            dimension=dimension,
            maximum=maximum,
            remediation=remediation,
        )

        return graph_work_budget_exceeded_error
