from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.changefeed_predicate import ChangefeedPredicate


T = TypeVar("T", bound="ChangefeedSelector")


@_attrs_define
class ChangefeedSelector:
    """
    Attributes:
        table (str):
        fields (list[str] | Unset):
        predicates (list[ChangefeedPredicate] | Unset):
    """

    table: str
    fields: list[str] | Unset = UNSET
    predicates: list[ChangefeedPredicate] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        predicates: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.predicates, Unset):
            predicates = []
            for predicates_item_data in self.predicates:
                predicates_item = predicates_item_data.to_dict()
                predicates.append(predicates_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
            }
        )
        if fields is not UNSET:
            field_dict["fields"] = fields
        if predicates is not UNSET:
            field_dict["predicates"] = predicates

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.changefeed_predicate import ChangefeedPredicate

        d = dict(src_dict)
        table = d.pop("table")

        fields = cast(list[str], d.pop("fields", UNSET))

        _predicates = d.pop("predicates", UNSET)
        predicates: list[ChangefeedPredicate] | Unset = UNSET
        if _predicates is not UNSET:
            predicates = []
            for predicates_item_data in _predicates:
                predicates_item = ChangefeedPredicate.from_dict(predicates_item_data)

                predicates.append(predicates_item)

        changefeed_selector = cls(
            table=table,
            fields=fields,
            predicates=predicates,
        )

        changefeed_selector.additional_properties = d
        return changefeed_selector

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
