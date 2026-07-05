from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sort_field import SortField


T = TypeVar("T", bound="SortProfile")


@_attrs_define
class SortProfile:
    """Sort execution profile. The fields below are the stable public
    diagnostic surface; profiling responses may include additional
    implementation counters.

        Attributes:
            plan (str | Unset): Stable physical sort plan name.
            order_by (list[SortField] | Unset): Requested order fields, including the implicit _id tie-breaker when
                applicable.
            cursor (str | Unset): Cursor mode for this request.
            exactness (str | Unset): Exactness class for the selected plan.
            source (str | Unset): Candidate source used by the selected plan.
            cursor_support (str | Unset): Cursor support level for the selected plan.
            source_load (str | Unset): Stored source load strategy.
            distributed_behavior (str | Unset): Distributed sort behavior.
            require_native (bool | Unset): Whether exact execution required native typed sort values.
            candidate_count (int | Unset): Candidate documents considered by sort execution.
            cursor_rejected_count (int | Unset): Candidates rejected by cursor comparison.
            selected_count (int | Unset): Hits selected for the returned page.
            total_us (int | Unset): Total sort execution time in microseconds.
            distributed_shard_count (int | Unset): Shards participating in distributed sort execution.
            budget_rejection_reason (str | Unset): Stable budget rejection reason.
            sort_rejection_reason (str | Unset): Stable exact-sort rejection reason.
            sort_rejection_detail (str | Unset): Stable rejection detail.
            sort_rejection_field (str | Unset): Sort field associated with the rejection when safe to expose.
    """

    plan: str | Unset = UNSET
    order_by: list[SortField] | Unset = UNSET
    cursor: str | Unset = UNSET
    exactness: str | Unset = UNSET
    source: str | Unset = UNSET
    cursor_support: str | Unset = UNSET
    source_load: str | Unset = UNSET
    distributed_behavior: str | Unset = UNSET
    require_native: bool | Unset = UNSET
    candidate_count: int | Unset = UNSET
    cursor_rejected_count: int | Unset = UNSET
    selected_count: int | Unset = UNSET
    total_us: int | Unset = UNSET
    distributed_shard_count: int | Unset = UNSET
    budget_rejection_reason: str | Unset = UNSET
    sort_rejection_reason: str | Unset = UNSET
    sort_rejection_detail: str | Unset = UNSET
    sort_rejection_field: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        plan = self.plan

        order_by: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.order_by, Unset):
            order_by = []
            for order_by_item_data in self.order_by:
                order_by_item = order_by_item_data.to_dict()
                order_by.append(order_by_item)

        cursor = self.cursor

        exactness = self.exactness

        source = self.source

        cursor_support = self.cursor_support

        source_load = self.source_load

        distributed_behavior = self.distributed_behavior

        require_native = self.require_native

        candidate_count = self.candidate_count

        cursor_rejected_count = self.cursor_rejected_count

        selected_count = self.selected_count

        total_us = self.total_us

        distributed_shard_count = self.distributed_shard_count

        budget_rejection_reason = self.budget_rejection_reason

        sort_rejection_reason = self.sort_rejection_reason

        sort_rejection_detail = self.sort_rejection_detail

        sort_rejection_field = self.sort_rejection_field

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if plan is not UNSET:
            field_dict["plan"] = plan
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if exactness is not UNSET:
            field_dict["exactness"] = exactness
        if source is not UNSET:
            field_dict["source"] = source
        if cursor_support is not UNSET:
            field_dict["cursor_support"] = cursor_support
        if source_load is not UNSET:
            field_dict["source_load"] = source_load
        if distributed_behavior is not UNSET:
            field_dict["distributed_behavior"] = distributed_behavior
        if require_native is not UNSET:
            field_dict["require_native"] = require_native
        if candidate_count is not UNSET:
            field_dict["candidate_count"] = candidate_count
        if cursor_rejected_count is not UNSET:
            field_dict["cursor_rejected_count"] = cursor_rejected_count
        if selected_count is not UNSET:
            field_dict["selected_count"] = selected_count
        if total_us is not UNSET:
            field_dict["total_us"] = total_us
        if distributed_shard_count is not UNSET:
            field_dict["distributed_shard_count"] = distributed_shard_count
        if budget_rejection_reason is not UNSET:
            field_dict["budget_rejection_reason"] = budget_rejection_reason
        if sort_rejection_reason is not UNSET:
            field_dict["sort_rejection_reason"] = sort_rejection_reason
        if sort_rejection_detail is not UNSET:
            field_dict["sort_rejection_detail"] = sort_rejection_detail
        if sort_rejection_field is not UNSET:
            field_dict["sort_rejection_field"] = sort_rejection_field

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sort_field import SortField

        d = dict(src_dict)
        plan = d.pop("plan", UNSET)

        _order_by = d.pop("order_by", UNSET)
        order_by: list[SortField] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:
                order_by_item = SortField.from_dict(order_by_item_data)

                order_by.append(order_by_item)

        cursor = d.pop("cursor", UNSET)

        exactness = d.pop("exactness", UNSET)

        source = d.pop("source", UNSET)

        cursor_support = d.pop("cursor_support", UNSET)

        source_load = d.pop("source_load", UNSET)

        distributed_behavior = d.pop("distributed_behavior", UNSET)

        require_native = d.pop("require_native", UNSET)

        candidate_count = d.pop("candidate_count", UNSET)

        cursor_rejected_count = d.pop("cursor_rejected_count", UNSET)

        selected_count = d.pop("selected_count", UNSET)

        total_us = d.pop("total_us", UNSET)

        distributed_shard_count = d.pop("distributed_shard_count", UNSET)

        budget_rejection_reason = d.pop("budget_rejection_reason", UNSET)

        sort_rejection_reason = d.pop("sort_rejection_reason", UNSET)

        sort_rejection_detail = d.pop("sort_rejection_detail", UNSET)

        sort_rejection_field = d.pop("sort_rejection_field", UNSET)

        sort_profile = cls(
            plan=plan,
            order_by=order_by,
            cursor=cursor,
            exactness=exactness,
            source=source,
            cursor_support=cursor_support,
            source_load=source_load,
            distributed_behavior=distributed_behavior,
            require_native=require_native,
            candidate_count=candidate_count,
            cursor_rejected_count=cursor_rejected_count,
            selected_count=selected_count,
            total_us=total_us,
            distributed_shard_count=distributed_shard_count,
            budget_rejection_reason=budget_rejection_reason,
            sort_rejection_reason=sort_rejection_reason,
            sort_rejection_detail=sort_rejection_detail,
            sort_rejection_field=sort_rejection_field,
        )

        sort_profile.additional_properties = d
        return sort_profile

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
