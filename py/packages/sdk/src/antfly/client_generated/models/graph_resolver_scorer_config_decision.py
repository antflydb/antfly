from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphResolverScorerConfigDecision")


@_attrs_define
class GraphResolverScorerConfigDecision:
    """
    Attributes:
        match (float | Unset):
        review (float | Unset):
    """

    match: float | Unset = UNSET
    review: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        match = self.match

        review = self.review

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if match is not UNSET:
            field_dict["match"] = match
        if review is not UNSET:
            field_dict["review"] = review

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        match = d.pop("match", UNSET)

        review = d.pop("review", UNSET)

        graph_resolver_scorer_config_decision = cls(
            match=match,
            review=review,
        )

        return graph_resolver_scorer_config_decision
