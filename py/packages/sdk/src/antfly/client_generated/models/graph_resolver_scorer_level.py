from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphResolverScorerLevel")


@_attrs_define
class GraphResolverScorerLevel:
    """
    Attributes:
        weight (float):
        when (str | Unset): Matcher condition, such as 'exact' or 'jaro_winkler >= 0.9'.
        else_ (bool | Unset): Catch-all level when no previous condition matched.
    """

    weight: float
    when: str | Unset = UNSET
    else_: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        weight = self.weight

        when = self.when

        else_ = self.else_

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "weight": weight,
            }
        )
        if when is not UNSET:
            field_dict["when"] = when
        if else_ is not UNSET:
            field_dict["else"] = else_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        weight = d.pop("weight")

        when = d.pop("when", UNSET)

        else_ = d.pop("else", UNSET)

        graph_resolver_scorer_level = cls(
            weight=weight,
            when=when,
            else_=else_,
        )

        return graph_resolver_scorer_level
