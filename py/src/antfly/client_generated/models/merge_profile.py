from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.merge_strategy import MergeStrategy
from ..types import UNSET, Unset

T = TypeVar("T", bound="MergeProfile")


@_attrs_define
class MergeProfile:
    """Result merge statistics for hybrid search.

    Attributes:
        strategy (Union[Unset, MergeStrategy]): Merge strategy for combining results from the semantic_search and
            full_text_search.
            rrf: Reciprocal Rank Fusion - combines scores using reciprocal rank formula
            rsf: Relative Score Fusion - normalizes scores by min/max within a window and combines weighted scores
            failover: Use full_text_search if embedding generation fails
        full_text_hits (Union[Unset, int]): Number of hits from full-text search before merge.
        semantic_hits (Union[Unset, int]): Number of hits from semantic search before merge.
        duration_ms (Union[Unset, int]): Time spent merging results in milliseconds.
    """

    strategy: Union[Unset, MergeStrategy] = UNSET
    full_text_hits: Union[Unset, int] = UNSET
    semantic_hits: Union[Unset, int] = UNSET
    duration_ms: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        strategy: Union[Unset, str] = UNSET
        if not isinstance(self.strategy, Unset):
            strategy = self.strategy.value

        full_text_hits = self.full_text_hits

        semantic_hits = self.semantic_hits

        duration_ms = self.duration_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if strategy is not UNSET:
            field_dict["strategy"] = strategy
        if full_text_hits is not UNSET:
            field_dict["full_text_hits"] = full_text_hits
        if semantic_hits is not UNSET:
            field_dict["semantic_hits"] = semantic_hits
        if duration_ms is not UNSET:
            field_dict["duration_ms"] = duration_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _strategy = d.pop("strategy", UNSET)
        strategy: Union[Unset, MergeStrategy]
        if isinstance(_strategy, Unset):
            strategy = UNSET
        else:
            strategy = MergeStrategy(_strategy)

        full_text_hits = d.pop("full_text_hits", UNSET)

        semantic_hits = d.pop("semantic_hits", UNSET)

        duration_ms = d.pop("duration_ms", UNSET)

        merge_profile = cls(
            strategy=strategy,
            full_text_hits=full_text_hits,
            semantic_hits=semantic_hits,
            duration_ms=duration_ms,
        )

        merge_profile.additional_properties = d
        return merge_profile

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
