from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryRecallRequestGraphType0")


@_attrs_define
class MemoryRecallRequestGraphType0:
    """
    Attributes:
        entity_seeds (list[str] | Unset):
        max_hops (int | Unset):  Default: 2.
        max_nodes (int | Unset):  Default: 256.
        max_edges (int | Unset):  Default: 1024.
        iterations (int | Unset):  Default: 24.
        restart_probability (float | Unset):  Default: 0.15.
        relation_weight (float | Unset):  Default: 0.5.
        provisional_weight (float | Unset):  Default: 0.5.
        include_episodes (bool | Unset):  Default: True.
    """

    entity_seeds: list[str] | Unset = UNSET
    max_hops: int | Unset = 2
    max_nodes: int | Unset = 256
    max_edges: int | Unset = 1024
    iterations: int | Unset = 24
    restart_probability: float | Unset = 0.15
    relation_weight: float | Unset = 0.5
    provisional_weight: float | Unset = 0.5
    include_episodes: bool | Unset = True

    def to_dict(self) -> dict[str, Any]:
        entity_seeds: list[str] | Unset = UNSET
        if not isinstance(self.entity_seeds, Unset):
            entity_seeds = self.entity_seeds

        max_hops = self.max_hops

        max_nodes = self.max_nodes

        max_edges = self.max_edges

        iterations = self.iterations

        restart_probability = self.restart_probability

        relation_weight = self.relation_weight

        provisional_weight = self.provisional_weight

        include_episodes = self.include_episodes

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if entity_seeds is not UNSET:
            field_dict["entity_seeds"] = entity_seeds
        if max_hops is not UNSET:
            field_dict["max_hops"] = max_hops
        if max_nodes is not UNSET:
            field_dict["max_nodes"] = max_nodes
        if max_edges is not UNSET:
            field_dict["max_edges"] = max_edges
        if iterations is not UNSET:
            field_dict["iterations"] = iterations
        if restart_probability is not UNSET:
            field_dict["restart_probability"] = restart_probability
        if relation_weight is not UNSET:
            field_dict["relation_weight"] = relation_weight
        if provisional_weight is not UNSET:
            field_dict["provisional_weight"] = provisional_weight
        if include_episodes is not UNSET:
            field_dict["include_episodes"] = include_episodes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        entity_seeds = cast(list[str], d.pop("entity_seeds", UNSET))

        max_hops = d.pop("max_hops", UNSET)

        max_nodes = d.pop("max_nodes", UNSET)

        max_edges = d.pop("max_edges", UNSET)

        iterations = d.pop("iterations", UNSET)

        restart_probability = d.pop("restart_probability", UNSET)

        relation_weight = d.pop("relation_weight", UNSET)

        provisional_weight = d.pop("provisional_weight", UNSET)

        include_episodes = d.pop("include_episodes", UNSET)

        memory_recall_request_graph_type_0 = cls(
            entity_seeds=entity_seeds,
            max_hops=max_hops,
            max_nodes=max_nodes,
            max_edges=max_edges,
            iterations=iterations,
            restart_probability=restart_probability,
            relation_weight=relation_weight,
            provisional_weight=provisional_weight,
            include_episodes=include_episodes,
        )

        return memory_recall_request_graph_type_0
