from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_path_weight_domain_error_error import GraphPathWeightDomainErrorError
from ..models.graph_path_weight_domain_error_mode import GraphPathWeightDomainErrorMode
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
        mode (GraphPathWeightDomainErrorMode): Exact path algorithm whose numeric domain was violated.
        violation (GraphPathWeightDomainErrorViolation): Stable machine-readable reason the weight was rejected.
        allowed_range (str): Required edge-weight interval for exact execution in the selected mode.
        remediation (str): Stable user-facing guidance for correcting the graph or query.
    """

    status: GraphPathWeightDomainErrorStatus
    error: GraphPathWeightDomainErrorError
    message: str
    retryable: bool
    operation: str
    mode: GraphPathWeightDomainErrorMode
    violation: GraphPathWeightDomainErrorViolation
    allowed_range: str
    remediation: str

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        operation = self.operation

        mode = self.mode.value

        violation = self.violation.value

        allowed_range = self.allowed_range

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
                "violation": violation,
                "allowed_range": allowed_range,
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

        mode = GraphPathWeightDomainErrorMode(d.pop("mode"))

        violation = GraphPathWeightDomainErrorViolation(d.pop("violation"))

        allowed_range = d.pop("allowed_range")

        remediation = d.pop("remediation")

        graph_path_weight_domain_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
            operation=operation,
            mode=mode,
            violation=violation,
            allowed_range=allowed_range,
            remediation=remediation,
        )

        return graph_path_weight_domain_error
