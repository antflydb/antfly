from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="AutocompleteQuery")


@_attrs_define
class AutocompleteQuery:
    """Analyzed bool-prefix search on a logical field. The planner selects
    declared search-as-you-type root, shingle and prefix representations.
    Requires a compatible representation; never silently becomes raw prefix search.

        Attributes:
            autocomplete (str):
            field (str):
            boost (float | None | Unset): A floating-point number used to decrease or increase the relevance scores of a
                query.
    """

    autocomplete: str
    field: str
    boost: float | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        autocomplete = self.autocomplete

        field = self.field

        boost: float | None | Unset
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "autocomplete": autocomplete,
                "field": field,
            }
        )
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        if set(src_dict) - {
            "autocomplete",
            "field",
            "boost",
        }:
            raise ValueError("Unexpected properties for AutocompleteQuery")
        d = dict(src_dict)
        autocomplete = d.pop("autocomplete")

        field = d.pop("field")

        def _parse_boost(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        boost = _parse_boost(d.pop("boost", UNSET))

        autocomplete_query = cls(
            autocomplete=autocomplete,
            field=field,
            boost=boost,
        )

        return autocomplete_query
