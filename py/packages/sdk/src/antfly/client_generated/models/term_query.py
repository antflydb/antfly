from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TermQuery")


@_attrs_define
class TermQuery:
    """Match an indexed post-analysis term exactly. Use match for analyzed natural-language input.

    Attributes:
        term (str):
        case_insensitive (bool | Unset): Fold ASCII letter case when matching indexed terms, without stemming the
            pattern. Default: False.
        field (str | Unset):
        boost (float | None | Unset): A floating-point number used to decrease or increase the relevance scores of a
            query.
    """

    term: str
    case_insensitive: bool | Unset = False
    field: str | Unset = UNSET
    boost: float | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        term = self.term

        case_insensitive = self.case_insensitive

        field = self.field

        boost: float | None | Unset
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "term": term,
            }
        )
        if case_insensitive is not UNSET:
            field_dict["case_insensitive"] = case_insensitive
        if field is not UNSET:
            field_dict["field"] = field
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        term = d.pop("term")

        case_insensitive = d.pop("case_insensitive", UNSET)

        field = d.pop("field", UNSET)

        def _parse_boost(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        boost = _parse_boost(d.pop("boost", UNSET))

        term_query = cls(
            term=term,
            case_insensitive=case_insensitive,
            field=field,
            boost=boost,
        )

        term_query.additional_properties = d
        return term_query

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
