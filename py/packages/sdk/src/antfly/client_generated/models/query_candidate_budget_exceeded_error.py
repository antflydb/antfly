from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.query_candidate_budget_exceeded_error_error import QueryCandidateBudgetExceededErrorError
from ..models.query_candidate_budget_exceeded_error_status import QueryCandidateBudgetExceededErrorStatus

T = TypeVar("T", bound="QueryCandidateBudgetExceededError")


@_attrs_define
class QueryCandidateBudgetExceededError:
    """
    Attributes:
        error (QueryCandidateBudgetExceededErrorError):
        message (str):
        reason (str):
        budget_rejection_reason (str):
        sort_rejection_reason (str):
        sort_rejection_detail (str):
        sort_rejection_field (str):
        status (QueryCandidateBudgetExceededErrorStatus):
    """

    error: QueryCandidateBudgetExceededErrorError
    message: str
    reason: str
    budget_rejection_reason: str
    sort_rejection_reason: str
    sort_rejection_detail: str
    sort_rejection_field: str
    status: QueryCandidateBudgetExceededErrorStatus

    def to_dict(self) -> dict[str, Any]:
        error = self.error.value

        message = self.message

        reason = self.reason

        budget_rejection_reason = self.budget_rejection_reason

        sort_rejection_reason = self.sort_rejection_reason

        sort_rejection_detail = self.sort_rejection_detail

        sort_rejection_field = self.sort_rejection_field

        status = self.status.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "error": error,
                "message": message,
                "reason": reason,
                "budget_rejection_reason": budget_rejection_reason,
                "sort_rejection_reason": sort_rejection_reason,
                "sort_rejection_detail": sort_rejection_detail,
                "sort_rejection_field": sort_rejection_field,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        error = QueryCandidateBudgetExceededErrorError(d.pop("error"))

        message = d.pop("message")

        reason = d.pop("reason")

        budget_rejection_reason = d.pop("budget_rejection_reason")

        sort_rejection_reason = d.pop("sort_rejection_reason")

        sort_rejection_detail = d.pop("sort_rejection_detail")

        sort_rejection_field = d.pop("sort_rejection_field")

        status = QueryCandidateBudgetExceededErrorStatus(d.pop("status"))

        query_candidate_budget_exceeded_error = cls(
            error=error,
            message=message,
            reason=reason,
            budget_rejection_reason=budget_rejection_reason,
            sort_rejection_reason=sort_rejection_reason,
            sort_rejection_detail=sort_rejection_detail,
            sort_rejection_field=sort_rejection_field,
            status=status,
        )

        return query_candidate_budget_exceeded_error
