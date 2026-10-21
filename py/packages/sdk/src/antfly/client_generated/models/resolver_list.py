from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.resolver_summary import ResolverSummary


T = TypeVar("T", bound="ResolverList")


@_attrs_define
class ResolverList:
    """
    Attributes:
        resolvers (list[ResolverSummary]):
    """

    resolvers: list[ResolverSummary]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resolvers = []
        for resolvers_item_data in self.resolvers:
            resolvers_item = resolvers_item_data.to_dict()
            resolvers.append(resolvers_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "resolvers": resolvers,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_summary import ResolverSummary

        d = dict(src_dict)
        resolvers = []
        _resolvers = d.pop("resolvers")
        for resolvers_item_data in _resolvers:
            resolvers_item = ResolverSummary.from_dict(resolvers_item_data)

            resolvers.append(resolvers_item)

        resolver_list = cls(
            resolvers=resolvers,
        )

        resolver_list.additional_properties = d
        return resolver_list

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
