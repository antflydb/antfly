from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_metric_config_kind import GraphMetricConfigKind
from ..models.graph_metric_config_refresh import GraphMetricConfigRefresh
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_metric_edge_filter import GraphMetricEdgeFilter


T = TypeVar("T", bound="GraphMetricConfig")


@_attrs_define
class GraphMetricConfig:
    """Published metric configuration. If kind is omitted, the metric name must be a supported kind.

    Attributes:
        enabled (bool | Unset):  Default: True.
        kind (GraphMetricConfigKind | Unset):
        refresh (GraphMetricConfigRefresh | Unset): Serverless accepts background only. Default:
            GraphMetricConfigRefresh.BACKGROUND.
        damping (float | Unset):  Default: 0.85.
        tolerance (float | Unset):  Default: 1e-06.
        max_iterations (int | Unset):  Default: 50.
        edge_filter (GraphMetricEdgeFilter | Unset): Omitting this object selects all edge types. A types list selects
            only those types; mode and types cannot both be supplied.
    """

    enabled: bool | Unset = True
    kind: GraphMetricConfigKind | Unset = UNSET
    refresh: GraphMetricConfigRefresh | Unset = GraphMetricConfigRefresh.BACKGROUND
    damping: float | Unset = 0.85
    tolerance: float | Unset = 1e-06
    max_iterations: int | Unset = 50
    edge_filter: GraphMetricEdgeFilter | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        refresh: str | Unset = UNSET
        if not isinstance(self.refresh, Unset):
            refresh = self.refresh.value

        damping = self.damping

        tolerance = self.tolerance

        max_iterations = self.max_iterations

        edge_filter: dict[str, Any] | Unset = UNSET
        if not isinstance(self.edge_filter, Unset):
            edge_filter = self.edge_filter.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if enabled is not UNSET:
            field_dict["enabled"] = enabled
        if kind is not UNSET:
            field_dict["kind"] = kind
        if refresh is not UNSET:
            field_dict["refresh"] = refresh
        if damping is not UNSET:
            field_dict["damping"] = damping
        if tolerance is not UNSET:
            field_dict["tolerance"] = tolerance
        if max_iterations is not UNSET:
            field_dict["max_iterations"] = max_iterations
        if edge_filter is not UNSET:
            field_dict["edge_filter"] = edge_filter

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_metric_edge_filter import GraphMetricEdgeFilter

        d = dict(src_dict)
        enabled = d.pop("enabled", UNSET)

        _kind = d.pop("kind", UNSET)
        kind: GraphMetricConfigKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = GraphMetricConfigKind(_kind)

        _refresh = d.pop("refresh", UNSET)
        refresh: GraphMetricConfigRefresh | Unset
        if isinstance(_refresh, Unset):
            refresh = UNSET
        else:
            refresh = GraphMetricConfigRefresh(_refresh)

        damping = d.pop("damping", UNSET)

        tolerance = d.pop("tolerance", UNSET)

        max_iterations = d.pop("max_iterations", UNSET)

        _edge_filter = d.pop("edge_filter", UNSET)
        edge_filter: GraphMetricEdgeFilter | Unset
        if isinstance(_edge_filter, Unset):
            edge_filter = UNSET
        else:
            edge_filter = GraphMetricEdgeFilter.from_dict(_edge_filter)

        graph_metric_config = cls(
            enabled=enabled,
            kind=kind,
            refresh=refresh,
            damping=damping,
            tolerance=tolerance,
            max_iterations=max_iterations,
            edge_filter=edge_filter,
        )

        return graph_metric_config
