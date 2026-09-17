from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionJointOptions")


@_attrs_define
class ExtractionJointOptions:
    """JointIE proposal admission and utility calibration. Entity candidate caps are bypassed for endpoints of retained
    relation proposals, subject to server hard bounds. entity_threshold overrides candidate admission, not entity
    decision thresholds.

        Attributes:
            candidate_threshold (float | Unset):
            entity_threshold (float | Unset):
            relation_role_threshold (float | Unset):
            top_k_entities (int | Unset):
            top_k_roles (int | Unset):
            relation_pair_cap (int | Unset):
            max_edges_per_type (int | Unset):
            entity_weight (float | Unset):
            relation_weight (float | Unset):
    """

    candidate_threshold: float | Unset = UNSET
    entity_threshold: float | Unset = UNSET
    relation_role_threshold: float | Unset = UNSET
    top_k_entities: int | Unset = UNSET
    top_k_roles: int | Unset = UNSET
    relation_pair_cap: int | Unset = UNSET
    max_edges_per_type: int | Unset = UNSET
    entity_weight: float | Unset = UNSET
    relation_weight: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        candidate_threshold = self.candidate_threshold

        entity_threshold = self.entity_threshold

        relation_role_threshold = self.relation_role_threshold

        top_k_entities = self.top_k_entities

        top_k_roles = self.top_k_roles

        relation_pair_cap = self.relation_pair_cap

        max_edges_per_type = self.max_edges_per_type

        entity_weight = self.entity_weight

        relation_weight = self.relation_weight

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if candidate_threshold is not UNSET:
            field_dict["candidate_threshold"] = candidate_threshold
        if entity_threshold is not UNSET:
            field_dict["entity_threshold"] = entity_threshold
        if relation_role_threshold is not UNSET:
            field_dict["relation_role_threshold"] = relation_role_threshold
        if top_k_entities is not UNSET:
            field_dict["top_k_entities"] = top_k_entities
        if top_k_roles is not UNSET:
            field_dict["top_k_roles"] = top_k_roles
        if relation_pair_cap is not UNSET:
            field_dict["relation_pair_cap"] = relation_pair_cap
        if max_edges_per_type is not UNSET:
            field_dict["max_edges_per_type"] = max_edges_per_type
        if entity_weight is not UNSET:
            field_dict["entity_weight"] = entity_weight
        if relation_weight is not UNSET:
            field_dict["relation_weight"] = relation_weight

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        candidate_threshold = d.pop("candidate_threshold", UNSET)

        entity_threshold = d.pop("entity_threshold", UNSET)

        relation_role_threshold = d.pop("relation_role_threshold", UNSET)

        top_k_entities = d.pop("top_k_entities", UNSET)

        top_k_roles = d.pop("top_k_roles", UNSET)

        relation_pair_cap = d.pop("relation_pair_cap", UNSET)

        max_edges_per_type = d.pop("max_edges_per_type", UNSET)

        entity_weight = d.pop("entity_weight", UNSET)

        relation_weight = d.pop("relation_weight", UNSET)

        extraction_joint_options = cls(
            candidate_threshold=candidate_threshold,
            entity_threshold=entity_threshold,
            relation_role_threshold=relation_role_threshold,
            top_k_entities=top_k_entities,
            top_k_roles=top_k_roles,
            relation_pair_cap=relation_pair_cap,
            max_edges_per_type=max_edges_per_type,
            entity_weight=entity_weight,
            relation_weight=relation_weight,
        )

        return extraction_joint_options
