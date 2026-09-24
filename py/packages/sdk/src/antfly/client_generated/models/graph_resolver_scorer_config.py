from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_resolver_scorer_comparison import GraphResolverScorerComparison
    from ..models.graph_resolver_scorer_config_combine import GraphResolverScorerConfigCombine
    from ..models.graph_resolver_scorer_config_decision import GraphResolverScorerConfigDecision


T = TypeVar("T", bound="GraphResolverScorerConfig")


@_attrs_define
class GraphResolverScorerConfig:
    """
    Attributes:
        comparisons (list[GraphResolverScorerComparison]):
        combine (GraphResolverScorerConfigCombine | Unset):
        decision (GraphResolverScorerConfigDecision | Unset):
    """

    comparisons: list[GraphResolverScorerComparison]
    combine: GraphResolverScorerConfigCombine | Unset = UNSET
    decision: GraphResolverScorerConfigDecision | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        comparisons = []
        for comparisons_item_data in self.comparisons:
            comparisons_item = comparisons_item_data.to_dict()
            comparisons.append(comparisons_item)

        combine: dict[str, Any] | Unset = UNSET
        if not isinstance(self.combine, Unset):
            combine = self.combine.to_dict()

        decision: dict[str, Any] | Unset = UNSET
        if not isinstance(self.decision, Unset):
            decision = self.decision.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "comparisons": comparisons,
            }
        )
        if combine is not UNSET:
            field_dict["combine"] = combine
        if decision is not UNSET:
            field_dict["decision"] = decision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_resolver_scorer_comparison import GraphResolverScorerComparison
        from ..models.graph_resolver_scorer_config_combine import GraphResolverScorerConfigCombine
        from ..models.graph_resolver_scorer_config_decision import GraphResolverScorerConfigDecision

        d = dict(src_dict)
        comparisons = []
        _comparisons = d.pop("comparisons")
        for comparisons_item_data in _comparisons:
            comparisons_item = GraphResolverScorerComparison.from_dict(comparisons_item_data)

            comparisons.append(comparisons_item)

        _combine = d.pop("combine", UNSET)
        combine: GraphResolverScorerConfigCombine | Unset
        if isinstance(_combine, Unset):
            combine = UNSET
        else:
            combine = GraphResolverScorerConfigCombine.from_dict(_combine)

        _decision = d.pop("decision", UNSET)
        decision: GraphResolverScorerConfigDecision | Unset
        if isinstance(_decision, Unset):
            decision = UNSET
        else:
            decision = GraphResolverScorerConfigDecision.from_dict(_decision)

        graph_resolver_scorer_config = cls(
            comparisons=comparisons,
            combine=combine,
            decision=decision,
        )

        return graph_resolver_scorer_config
