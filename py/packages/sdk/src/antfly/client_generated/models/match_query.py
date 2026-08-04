from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="MatchQuery")


@_attrs_define
class MatchQuery:
    """
    Attributes:
        match (str):
        field (str | Unset):
        analyzer (str | Unset):
        boost (float | None | Unset): A floating-point number used to decrease or increase the relevance scores of a
            query.
    """

    match: str
    field: str | Unset = UNSET
    analyzer: str | Unset = UNSET
    boost: float | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        match = self.match

        field = self.field

        analyzer = self.analyzer

        boost: float | None | Unset
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "match": match,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if analyzer is not UNSET:
            field_dict["analyzer"] = analyzer
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        match = d.pop("match")

        field = d.pop("field", UNSET)

        analyzer = d.pop("analyzer", UNSET)

        def _parse_boost(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        boost = _parse_boost(d.pop("boost", UNSET))

        match_query = cls(
            match=match,
            field=field,
            analyzer=analyzer,
            boost=boost,
        )

        match_query.additional_properties = d
        return match_query

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
