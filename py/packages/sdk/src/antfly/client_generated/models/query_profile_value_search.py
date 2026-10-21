from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_profile_value_search_plans_item import QueryProfileValueSearchPlansItem


T = TypeVar("T", bound="QueryProfileValueSearch")


@_attrs_define
class QueryProfileValueSearch:
    """Actual logical value verification work, summed across participating shards; plans describe selected physical fields
    and scan fallback reasons. Budgets remain per shard.

        Attributes:
            documents (int | Unset):
            bytes_ (int | Unset):
            plans (list[QueryProfileValueSearchPlansItem] | Unset):
    """

    documents: int | Unset = UNSET
    bytes_: int | Unset = UNSET
    plans: list[QueryProfileValueSearchPlansItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        documents = self.documents

        bytes_ = self.bytes_

        plans: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.plans, Unset):
            plans = []
            for plans_item_data in self.plans:
                plans_item = plans_item_data.to_dict()
                plans.append(plans_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if documents is not UNSET:
            field_dict["documents"] = documents
        if bytes_ is not UNSET:
            field_dict["bytes"] = bytes_
        if plans is not UNSET:
            field_dict["plans"] = plans

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_profile_value_search_plans_item import QueryProfileValueSearchPlansItem

        d = dict(src_dict)
        documents = d.pop("documents", UNSET)

        bytes_ = d.pop("bytes", UNSET)

        _plans = d.pop("plans", UNSET)
        plans: list[QueryProfileValueSearchPlansItem] | Unset = UNSET
        if _plans is not UNSET:
            plans = []
            for plans_item_data in _plans:
                plans_item = QueryProfileValueSearchPlansItem.from_dict(plans_item_data)

                plans.append(plans_item)

        query_profile_value_search = cls(
            documents=documents,
            bytes_=bytes_,
            plans=plans,
        )

        query_profile_value_search.additional_properties = d
        return query_profile_value_search

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
