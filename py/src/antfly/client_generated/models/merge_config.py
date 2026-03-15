from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.merge_strategy import MergeStrategy
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.merge_config_weights import MergeConfigWeights


T = TypeVar("T", bound="MergeConfig")


@_attrs_define
class MergeConfig:
    """Configuration for result fusion when combining multiple search indexes.

    Attributes:
        strategy (Union[Unset, MergeStrategy]): Merge strategy for combining results from the semantic_search and
            full_text_search.
            rrf: Reciprocal Rank Fusion - combines scores using reciprocal rank formula
            rsf: Relative Score Fusion - normalizes scores by min/max within a window and combines weighted scores
            failover: Use full_text_search if embedding generation fails
        weights (Union[Unset, MergeConfigWeights]): Named weights keyed by index name. `full_text` for the full-text
            search index;
            embedding index names for vector indexes.
            Unspecified indexes default to 1.0. Applied in both RRF and RSF.
             Example: {'full_text': 0.3, 'title_embedding': 1}.
        window_size (Union[Unset, int]): RSF normalization window size. Defaults to `limit`.
        rank_constant (Union[Unset, float]): RRF k constant (1/(k+rank)). Defaults to 60.0.
    """

    strategy: Union[Unset, MergeStrategy] = UNSET
    weights: Union[Unset, "MergeConfigWeights"] = UNSET
    window_size: Union[Unset, int] = UNSET
    rank_constant: Union[Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        strategy: Union[Unset, str] = UNSET
        if not isinstance(self.strategy, Unset):
            strategy = self.strategy.value

        weights: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.weights, Unset):
            weights = self.weights.to_dict()

        window_size = self.window_size

        rank_constant = self.rank_constant

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if strategy is not UNSET:
            field_dict["strategy"] = strategy
        if weights is not UNSET:
            field_dict["weights"] = weights
        if window_size is not UNSET:
            field_dict["window_size"] = window_size
        if rank_constant is not UNSET:
            field_dict["rank_constant"] = rank_constant

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.merge_config_weights import MergeConfigWeights

        d = dict(src_dict)
        _strategy = d.pop("strategy", UNSET)
        strategy: Union[Unset, MergeStrategy]
        if isinstance(_strategy, Unset):
            strategy = UNSET
        else:
            strategy = MergeStrategy(_strategy)

        _weights = d.pop("weights", UNSET)
        weights: Union[Unset, MergeConfigWeights]
        if isinstance(_weights, Unset):
            weights = UNSET
        else:
            weights = MergeConfigWeights.from_dict(_weights)

        window_size = d.pop("window_size", UNSET)

        rank_constant = d.pop("rank_constant", UNSET)

        merge_config = cls(
            strategy=strategy,
            weights=weights,
            window_size=window_size,
            rank_constant=rank_constant,
        )

        merge_config.additional_properties = d
        return merge_config

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
