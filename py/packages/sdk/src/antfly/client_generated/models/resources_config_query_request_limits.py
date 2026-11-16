from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_execution_config import GraphExecutionConfig


T = TypeVar("T", bound="ResourcesConfigQueryRequestLimits")


@_attrs_define
class ResourcesConfigQueryRequestLimits:
    """
    Attributes:
        graph (GraphExecutionConfig | Unset): Operator-owned ceilings shared by all named graph operations in one
            admitted query. Public requests cannot raise these values. Explored work, scanned anchors, and exact-distinct
            capacity are request-consumptive; intermediate and retained-state ceilings bound simultaneously live state.
    """

    graph: GraphExecutionConfig | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        graph: dict[str, Any] | Unset = UNSET
        if not isinstance(self.graph, Unset):
            graph = self.graph.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if graph is not UNSET:
            field_dict["graph"] = graph

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_execution_config import GraphExecutionConfig

        d = dict(src_dict)
        _graph = d.pop("graph", UNSET)
        graph: GraphExecutionConfig | Unset
        if isinstance(_graph, Unset):
            graph = UNSET
        else:
            graph = GraphExecutionConfig.from_dict(_graph)

        resources_config_query_request_limits = cls(
            graph=graph,
        )

        return resources_config_query_request_limits
