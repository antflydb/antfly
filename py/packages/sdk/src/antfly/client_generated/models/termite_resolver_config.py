from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TermiteResolverConfig")


@_attrs_define
class TermiteResolverConfig:
    """Configuration for entity resolution. When present in a RecognizeRequest,
    the response entities and relations are deduplicated via entity resolution
    (e.g., "Elon Musk" and "Musk" are merged into a single entity).

        Attributes:
            similarity_threshold (float | Unset): Jaro-Winkler similarity threshold for merging entities (0.0-1.0) Default:
                0.85.
            type_must_match (bool | Unset): Whether entity types must match for merging Default: True.
            min_entity_confidence (float | Unset): Minimum confidence score for entities to be included Default: 0.0.
            min_relation_confidence (float | Unset): Minimum confidence score for relations to be included Default: 0.0.
            deduplicate_relations (bool | Unset): Whether to deduplicate relations after entity resolution Default: True.
            track_provenance (bool | Unset): Whether to track mention provenance for resolved entities Default: True.
    """

    similarity_threshold: float | Unset = 0.85
    type_must_match: bool | Unset = True
    min_entity_confidence: float | Unset = 0.0
    min_relation_confidence: float | Unset = 0.0
    deduplicate_relations: bool | Unset = True
    track_provenance: bool | Unset = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        similarity_threshold = self.similarity_threshold

        type_must_match = self.type_must_match

        min_entity_confidence = self.min_entity_confidence

        min_relation_confidence = self.min_relation_confidence

        deduplicate_relations = self.deduplicate_relations

        track_provenance = self.track_provenance

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if similarity_threshold is not UNSET:
            field_dict["similarity_threshold"] = similarity_threshold
        if type_must_match is not UNSET:
            field_dict["type_must_match"] = type_must_match
        if min_entity_confidence is not UNSET:
            field_dict["min_entity_confidence"] = min_entity_confidence
        if min_relation_confidence is not UNSET:
            field_dict["min_relation_confidence"] = min_relation_confidence
        if deduplicate_relations is not UNSET:
            field_dict["deduplicate_relations"] = deduplicate_relations
        if track_provenance is not UNSET:
            field_dict["track_provenance"] = track_provenance

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        similarity_threshold = d.pop("similarity_threshold", UNSET)

        type_must_match = d.pop("type_must_match", UNSET)

        min_entity_confidence = d.pop("min_entity_confidence", UNSET)

        min_relation_confidence = d.pop("min_relation_confidence", UNSET)

        deduplicate_relations = d.pop("deduplicate_relations", UNSET)

        track_provenance = d.pop("track_provenance", UNSET)

        termite_resolver_config = cls(
            similarity_threshold=similarity_threshold,
            type_must_match=type_must_match,
            min_entity_confidence=min_entity_confidence,
            min_relation_confidence=min_relation_confidence,
            deduplicate_relations=deduplicate_relations,
            track_provenance=track_provenance,
        )

        termite_resolver_config.additional_properties = d
        return termite_resolver_config

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
