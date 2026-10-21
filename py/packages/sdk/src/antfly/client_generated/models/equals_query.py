from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="EqualsQuery")


@_attrs_define
class EqualsQuery:
    """Whole string equality on the original logical field value.
    No normalization is applied. Case-sensitive by default; case_sensitive=false
    uses Unicode simple case folding, not locale-specific or multi-character folding.
    Missing, null, and non-string values do not match. Arrays use any-element
    semantics; a match never spans array elements. Indexes select conservative
    candidates and original values are verified before counting and pagination.

        Attributes:
            equals (str):
            field (str): Logical source field path, not a companion index name.
            case_sensitive (bool | Unset):  Default: True.
            boost (float | None | Unset): A floating-point number used to decrease or increase the relevance scores of a
                query.
    """

    equals: str
    field: str
    case_sensitive: bool | Unset = True
    boost: float | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        equals = self.equals

        field = self.field

        case_sensitive = self.case_sensitive

        boost: float | None | Unset
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "equals": equals,
                "field": field,
            }
        )
        if case_sensitive is not UNSET:
            field_dict["case_sensitive"] = case_sensitive
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        if set(src_dict) - {
            "equals",
            "field",
            "case_sensitive",
            "boost",
        }:
            raise ValueError("Unexpected properties for EqualsQuery")
        d = dict(src_dict)
        equals = d.pop("equals")

        field = d.pop("field")

        case_sensitive = d.pop("case_sensitive", UNSET)

        def _parse_boost(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        boost = _parse_boost(d.pop("boost", UNSET))

        equals_query = cls(
            equals=equals,
            field=field,
            case_sensitive=case_sensitive,
            boost=boost,
        )

        return equals_query
