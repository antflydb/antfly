from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.resolver_distinct_pair import ResolverDistinctPair
    from ..models.resolver_redirect import ResolverRedirect


T = TypeVar("T", bound="ResolverRedirectList")


@_attrs_define
class ResolverRedirectList:
    """
    Attributes:
        redirects (list[ResolverRedirect]):
        distinct (list[ResolverDistinctPair]):
        revision (int):
    """

    redirects: list[ResolverRedirect]
    distinct: list[ResolverDistinctPair]
    revision: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        redirects = []
        for redirects_item_data in self.redirects:
            redirects_item = redirects_item_data.to_dict()
            redirects.append(redirects_item)

        distinct = []
        for distinct_item_data in self.distinct:
            distinct_item = distinct_item_data.to_dict()
            distinct.append(distinct_item)

        revision = self.revision

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "redirects": redirects,
                "distinct": distinct,
                "revision": revision,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_distinct_pair import ResolverDistinctPair
        from ..models.resolver_redirect import ResolverRedirect

        d = dict(src_dict)
        redirects = []
        _redirects = d.pop("redirects")
        for redirects_item_data in _redirects:
            redirects_item = ResolverRedirect.from_dict(redirects_item_data)

            redirects.append(redirects_item)

        distinct = []
        _distinct = d.pop("distinct")
        for distinct_item_data in _distinct:
            distinct_item = ResolverDistinctPair.from_dict(distinct_item_data)

            distinct.append(distinct_item)

        revision = d.pop("revision")

        resolver_redirect_list = cls(
            redirects=redirects,
            distinct=distinct,
            revision=revision,
        )

        resolver_redirect_list.additional_properties = d
        return resolver_redirect_list

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
