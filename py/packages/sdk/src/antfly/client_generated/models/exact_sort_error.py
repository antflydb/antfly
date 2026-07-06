from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExactSortError")


@_attrs_define
class ExactSortError:
    """
    Attributes:
        error (str): Stable error class. Example: unsupported_exact_sort.
        message (str): Human-readable error summary. Example: exact sort is unsupported for this query.
        reason (str): Stable machine-readable rejection reason. Known exact-sort
            reasons include `unmapped_sort_field`,
            `non_sortable_sort_field`, `missing_doc_values_coverage`,
            `missing_native_filter_coverage`, `invalid_cursor_arity`,
            `invalid_cursor_type`, `approximate_candidate_source`,
            `candidate_budget_exceeded`, `count_only_ordered_page`,
            `stored_json_sort_disabled`, and
            `distributed_merge_unsupported`.
             Example: missing_doc_values_coverage.
        sort_rejection_reason (str): Stable exact-sort rejection reason; uses the same stable reason taxonomy as
            `reason`. Example: missing_doc_values_coverage.
        sort_rejection_detail (str): More specific exact-sort rejection detail. Example: missing_doc_values_section.
        sort_rejection_field (str): Sort field associated with the rejection when safe to expose. Example: created_at.
        status (int):  Example: 422.
        budget_rejection_reason (str | Unset): Stable budget rejection reason when the rejection was budget-driven.
            Example: text_field_sort_candidate_window.
    """

    error: str
    message: str
    reason: str
    sort_rejection_reason: str
    sort_rejection_detail: str
    sort_rejection_field: str
    status: int
    budget_rejection_reason: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        error = self.error

        message = self.message

        reason = self.reason

        sort_rejection_reason = self.sort_rejection_reason

        sort_rejection_detail = self.sort_rejection_detail

        sort_rejection_field = self.sort_rejection_field

        status = self.status

        budget_rejection_reason = self.budget_rejection_reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "error": error,
                "message": message,
                "reason": reason,
                "sort_rejection_reason": sort_rejection_reason,
                "sort_rejection_detail": sort_rejection_detail,
                "sort_rejection_field": sort_rejection_field,
                "status": status,
            }
        )
        if budget_rejection_reason is not UNSET:
            field_dict["budget_rejection_reason"] = budget_rejection_reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        error = d.pop("error")

        message = d.pop("message")

        reason = d.pop("reason")

        sort_rejection_reason = d.pop("sort_rejection_reason")

        sort_rejection_detail = d.pop("sort_rejection_detail")

        sort_rejection_field = d.pop("sort_rejection_field")

        status = d.pop("status")

        budget_rejection_reason = d.pop("budget_rejection_reason", UNSET)

        exact_sort_error = cls(
            error=error,
            message=message,
            reason=reason,
            sort_rejection_reason=sort_rejection_reason,
            sort_rejection_detail=sort_rejection_detail,
            sort_rejection_field=sort_rejection_field,
            status=status,
            budget_rejection_reason=budget_rejection_reason,
        )

        exact_sort_error.additional_properties = d
        return exact_sort_error

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
