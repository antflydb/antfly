from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_paths_result_kind import GraphPathsResultKind

if TYPE_CHECKING:
    from ..models.graph_exact_result_stats import GraphExactResultStats
    from ..models.graph_path_result import GraphPathResult


T = TypeVar("T", bound="GraphPathsResult")


@_attrs_define
class GraphPathsResult:
    """Composable results from canonical shortest_path or k_shortest_paths queries.

    Attributes:
        kind (GraphPathsResultKind): Stable discriminator for the graph result shape.
        paths (list[GraphPathResult]):
        stats (GraphExactResultStats): Completion statistics for a graph result that is exact or fails without producing
            a result.
    """

    kind: GraphPathsResultKind
    paths: list[GraphPathResult]
    stats: GraphExactResultStats

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        paths = []
        for paths_item_data in self.paths:
            paths_item = paths_item_data.to_dict()
            paths.append(paths_item)

        stats = self.stats.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "paths": paths,
                "stats": stats,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_exact_result_stats import GraphExactResultStats
        from ..models.graph_path_result import GraphPathResult

        d = dict(src_dict)
        kind = GraphPathsResultKind(d.pop("kind"))

        paths = []
        _paths = d.pop("paths")
        for paths_item_data in _paths:
            paths_item = GraphPathResult.from_dict(paths_item_data)

            paths.append(paths_item)

        stats = GraphExactResultStats.from_dict(d.pop("stats"))

        graph_paths_result = cls(
            kind=kind,
            paths=paths,
            stats=stats,
        )

        return graph_paths_result
