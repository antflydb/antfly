from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.multi_match_body_type import MultiMatchBodyType
from ..types import UNSET, Unset

T = TypeVar("T", bound="MultiMatchBody")


@_attrs_define
class MultiMatchBody:
    """
    Attributes:
        query (str):
        fields (list[str]):
        type_ (MultiMatchBodyType):
        boost (float | None | Unset): A floating-point number used to decrease or increase the relevance scores of a
            query.
    """

    query: str
    fields: list[str]
    type_: MultiMatchBodyType
    boost: float | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        query = self.query

        fields = self.fields

        type_ = self.type_.value

        boost: float | None | Unset
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "query": query,
                "fields": fields,
                "type": type_,
            }
        )
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        query = d.pop("query")

        fields = cast(list[str], d.pop("fields"))

        type_ = MultiMatchBodyType(d.pop("type"))

        def _parse_boost(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        boost = _parse_boost(d.pop("boost", UNSET))

        multi_match_body = cls(
            query=query,
            fields=fields,
            type_=type_,
            boost=boost,
        )

        multi_match_body.additional_properties = d
        return multi_match_body

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
