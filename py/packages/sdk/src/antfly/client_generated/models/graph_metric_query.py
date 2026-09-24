from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_metric_query_metric_freshness import GraphMetricQueryMetricFreshness
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphMetricQuery")


@_attrs_define
class GraphMetricQuery:
    """Reads a published graph metric. Score-bearing graph metric queries on multi-shard tables require a globally
    coordinated metric snapshot and otherwise return graph_metric_global_materialization_required instead of merging
    mathematically incompatible shard-local scores.

        Attributes:
            index (str): Graph index that owns the published metric.
            metric (str): Graph metric to read.
            name (str | Unset): Optional result key. Defaults to the metric name.
            top_k (int | Unset): Maximum ranked metric scores to return. Multi-shard tables require a globally coordinated
                metric snapshot. Default: 10.
            metric_freshness (GraphMetricQueryMetricFreshness | Unset): Whether the latest published generation may be stale
                or must match the graph edge generation. Default: GraphMetricQueryMetricFreshness.PUBLISHED.
            seed_nodes (list[str] | Unset): Node keys receiving all teleport mass for query-seeded personalized PageRank
                (HippoRAG-style retrieval). Only valid for pagerank metrics and requires metric_freshness=fresh: personalized
                scores are computed at query time from the current edge snapshot, while published generations are global-only,
                so seeded reads against published freshness are rejected. Seed keys absent from the graph are skipped; if none
                resolve, ranking degenerates to global PageRank.
            damping (float | Unset): Damping override for query-seeded personalized PageRank (typical HippoRAG-style
                retrieval uses 0.9). Only valid together with seed_nodes; omitted reads keep the metric's configured damping.
    """

    index: str
    metric: str
    name: str | Unset = UNSET
    top_k: int | Unset = 10
    metric_freshness: GraphMetricQueryMetricFreshness | Unset = GraphMetricQueryMetricFreshness.PUBLISHED
    seed_nodes: list[str] | Unset = UNSET
    damping: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        metric = self.metric

        name = self.name

        top_k = self.top_k

        metric_freshness: str | Unset = UNSET
        if not isinstance(self.metric_freshness, Unset):
            metric_freshness = self.metric_freshness.value

        seed_nodes: list[str] | Unset = UNSET
        if not isinstance(self.seed_nodes, Unset):
            seed_nodes = self.seed_nodes

        damping = self.damping

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index": index,
                "metric": metric,
            }
        )
        if name is not UNSET:
            field_dict["name"] = name
        if top_k is not UNSET:
            field_dict["top_k"] = top_k
        if metric_freshness is not UNSET:
            field_dict["metric_freshness"] = metric_freshness
        if seed_nodes is not UNSET:
            field_dict["seed_nodes"] = seed_nodes
        if damping is not UNSET:
            field_dict["damping"] = damping

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index = d.pop("index")

        metric = d.pop("metric")

        name = d.pop("name", UNSET)

        top_k = d.pop("top_k", UNSET)

        _metric_freshness = d.pop("metric_freshness", UNSET)
        metric_freshness: GraphMetricQueryMetricFreshness | Unset
        if isinstance(_metric_freshness, Unset):
            metric_freshness = UNSET
        else:
            metric_freshness = GraphMetricQueryMetricFreshness(_metric_freshness)

        seed_nodes = cast(list[str], d.pop("seed_nodes", UNSET))

        damping = d.pop("damping", UNSET)

        graph_metric_query = cls(
            index=index,
            metric=metric,
            name=name,
            top_k=top_k,
            metric_freshness=metric_freshness,
            seed_nodes=seed_nodes,
            damping=damping,
        )

        graph_metric_query.additional_properties = d
        return graph_metric_query

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
