from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_path_objective import GraphPathObjective
from ..models.graph_path_weight_domain_error_error import GraphPathWeightDomainErrorError
from ..models.graph_path_weight_domain_error_status import GraphPathWeightDomainErrorStatus
from ..models.graph_path_weight_domain_error_violation import GraphPathWeightDomainErrorViolation

T = TypeVar("T", bound="GraphPathWeightDomainError")


@_attrs_define
class GraphPathWeightDomainError:
    """
    Attributes:
        status (GraphPathWeightDomainErrorStatus):
        error (GraphPathWeightDomainErrorError):
        message (str):
        retryable (bool):
        operation (str): Named shortest-path operation that encountered the incompatible edge weight.
        objective (GraphPathObjective): Objective used to rank graph paths:
            - min_hops: Minimize the number of edges.
            - min_weight_sum: Minimize the sum of finite non-negative edge weights.
            - max_weight_product: Maximize the product of edge weights, requiring every traversed weight to be in [0,1].
        violation (GraphPathWeightDomainErrorViolation): Stable machine-readable reason the weight was rejected.
        remediation (str): Stable user-facing guidance for correcting the graph or query.
    """

    status: GraphPathWeightDomainErrorStatus
    error: GraphPathWeightDomainErrorError
    message: str
    retryable: bool
    operation: str
    objective: GraphPathObjective
    violation: GraphPathWeightDomainErrorViolation
    remediation: str

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        operation = self.operation

        objective = self.objective.value

        violation = self.violation.value

        remediation = self.remediation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "retryable": retryable,
                "operation": operation,
                "objective": objective,
                "violation": violation,
                "remediation": remediation,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = GraphPathWeightDomainErrorStatus(d.pop("status"))

        error = GraphPathWeightDomainErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        operation = d.pop("operation")

        objective = GraphPathObjective(d.pop("objective"))

        violation = GraphPathWeightDomainErrorViolation(d.pop("violation"))

        remediation = d.pop("remediation")

        graph_path_weight_domain_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
            operation=operation,
            objective=objective,
            violation=violation,
            remediation=remediation,
        )

        return graph_path_weight_domain_error
