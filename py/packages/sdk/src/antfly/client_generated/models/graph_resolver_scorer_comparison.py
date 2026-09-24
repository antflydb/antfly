from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_resolver_scorer_level import GraphResolverScorerLevel


T = TypeVar("T", bound="GraphResolverScorerComparison")


@_attrs_define
class GraphResolverScorerComparison:
    """
    Attributes:
        name (str):
        left (str):
        right (str):
        levels (list[GraphResolverScorerLevel]):
    """

    name: str
    left: str
    right: str
    levels: list[GraphResolverScorerLevel]

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        left = self.left

        right = self.right

        levels = []
        for levels_item_data in self.levels:
            levels_item = levels_item_data.to_dict()
            levels.append(levels_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "left": left,
                "right": right,
                "levels": levels,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_resolver_scorer_level import GraphResolverScorerLevel

        d = dict(src_dict)
        name = d.pop("name")

        left = d.pop("left")

        right = d.pop("right")

        levels = []
        _levels = d.pop("levels")
        for levels_item_data in _levels:
            levels_item = GraphResolverScorerLevel.from_dict(levels_item_data)

            levels.append(levels_item)

        graph_resolver_scorer_comparison = cls(
            name=name,
            left=left,
            right=right,
            levels=levels,
        )

        return graph_resolver_scorer_comparison
