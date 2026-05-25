from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.multi_match_body import MultiMatchBody


T = TypeVar("T", bound="MultiMatchQuery")


@_attrs_define
class MultiMatchQuery:
    """
    Attributes:
        multi_match (MultiMatchBody):
    """

    multi_match: MultiMatchBody
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        multi_match = self.multi_match.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "multi_match": multi_match,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.multi_match_body import MultiMatchBody

        d = dict(src_dict)
        multi_match = MultiMatchBody.from_dict(d.pop("multi_match"))

        multi_match_query = cls(
            multi_match=multi_match,
        )

        multi_match_query.additional_properties = d
        return multi_match_query

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
