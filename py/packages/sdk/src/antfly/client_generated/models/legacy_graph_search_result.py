from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_query_type import GraphQueryType
from ..models.legacy_graph_search_result_kind import LegacyGraphSearchResultKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.legacy_graph_result_node import LegacyGraphResultNode
    from ..models.path import Path
    from ..models.pattern_match import PatternMatch


T = TypeVar("T", bound="LegacyGraphSearchResult")


@_attrs_define
class LegacyGraphSearchResult:
    """Deprecated graph_searches response envelope.

    Attributes:
        type_ (GraphQueryType): Deprecated discriminator used by LegacyGraphQuery.
        total (int): Deprecated graph_searches result count; use stats or a named count aggregate.
        kind (LegacyGraphSearchResultKind | Unset): Optional transition discriminator accepted by current SDKs. Servers
            omit it for graph_searches during the v0.2 compatibility release so strict previously generated clients continue
            to decode the original response shape.
        nodes (list[LegacyGraphResultNode] | Unset): Result nodes. Optional for compatibility with v0.2 responses.
        paths (list[Path] | Unset): Result paths. Optional for compatibility with v0.2 responses.
        matches (list[PatternMatch] | Unset): Deprecated graph_searches pattern results; use rows for graph_queries.
        took (int | Unset): Whole-query execution time in milliseconds; optional for compatibility with v0.2 responses.
            Use the parent query result's took field.
    """

    type_: GraphQueryType
    total: int
    kind: LegacyGraphSearchResultKind | Unset = UNSET
    nodes: list[LegacyGraphResultNode] | Unset = UNSET
    paths: list[Path] | Unset = UNSET
    matches: list[PatternMatch] | Unset = UNSET
    took: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        total = self.total

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        nodes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.nodes, Unset):
            nodes = []
            for nodes_item_data in self.nodes:
                nodes_item = nodes_item_data.to_dict()
                nodes.append(nodes_item)

        paths: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.paths, Unset):
            paths = []
            for paths_item_data in self.paths:
                paths_item = paths_item_data.to_dict()
                paths.append(paths_item)

        matches: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.matches, Unset):
            matches = []
            for matches_item_data in self.matches:
                matches_item = matches_item_data.to_dict()
                matches.append(matches_item)

        took = self.took

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "total": total,
            }
        )
        if kind is not UNSET:
            field_dict["kind"] = kind
        if nodes is not UNSET:
            field_dict["nodes"] = nodes
        if paths is not UNSET:
            field_dict["paths"] = paths
        if matches is not UNSET:
            field_dict["matches"] = matches
        if took is not UNSET:
            field_dict["took"] = took

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.legacy_graph_result_node import LegacyGraphResultNode
        from ..models.path import Path
        from ..models.pattern_match import PatternMatch

        d = dict(src_dict)
        type_ = GraphQueryType(d.pop("type"))

        total = d.pop("total")

        _kind = d.pop("kind", UNSET)
        kind: LegacyGraphSearchResultKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = LegacyGraphSearchResultKind(_kind)

        _nodes = d.pop("nodes", UNSET)
        nodes: list[LegacyGraphResultNode] | Unset = UNSET
        if _nodes is not UNSET:
            nodes = []
            for nodes_item_data in _nodes:
                nodes_item = LegacyGraphResultNode.from_dict(nodes_item_data)

                nodes.append(nodes_item)

        _paths = d.pop("paths", UNSET)
        paths: list[Path] | Unset = UNSET
        if _paths is not UNSET:
            paths = []
            for paths_item_data in _paths:
                paths_item = Path.from_dict(paths_item_data)

                paths.append(paths_item)

        _matches = d.pop("matches", UNSET)
        matches: list[PatternMatch] | Unset = UNSET
        if _matches is not UNSET:
            matches = []
            for matches_item_data in _matches:
                matches_item = PatternMatch.from_dict(matches_item_data)

                matches.append(matches_item)

        took = d.pop("took", UNSET)

        legacy_graph_search_result = cls(
            type_=type_,
            total=total,
            kind=kind,
            nodes=nodes,
            paths=paths,
            matches=matches,
            took=took,
        )

        return legacy_graph_search_result
