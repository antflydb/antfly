from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.edge_direction import EdgeDirection
from ..models.retrieval_navigation_selection import RetrievalNavigationSelection
from ..models.retrieval_navigation_strategy import RetrievalNavigationStrategy
from ..types import UNSET, Unset

T = TypeVar("T", bound="RetrievalNavigationConfig")


@_attrs_define
class RetrievalNavigationConfig:
    """Retrieval-step navigation targeting one ordinary query. Graph navigation
    follows one path; tree navigation explores a retained branch frontier.
    Agentic selection uses the enclosing model and budgets. Ranked selection
    is supported for trees and uses the existing deterministic tree traversal.
    Agentic selection requires agentic mode and a retrieval generator. Search
    starts exploration; navigation selects only an offered, unvisited node.
    All reads enforce mandatory predicates and authenticated row filters.

        Attributes:
            query_index (int): Zero-based index into the enclosing request queries.
            strategy (RetrievalNavigationStrategy):
            selection (RetrievalNavigationSelection):
            index (str): Graph index used for every neighbor read.
            max_depth (int | Unset): Tree-only maximum depth from the start node (depth zero); defaults to 5.
            beam_width (int | Unset): Tree-only maximum children offered per expansion; defaults to 3.
            start_nodes (str | Unset): Ranked-tree-only seed selector (comma-separated keys, $roots, or a prior-result
                selector). Mutually exclusive with start_key.
            start_key (str | Unset): Literal start document key. Agentic selection defaults to the first query hit; ranked
                selection defaults to seed results or prior query hits.
            direction (EdgeDirection | Unset): Direction of edges to query:
                - out: Outgoing edges from the node
                - in: Incoming edges to the node
                - both: Both outgoing and incoming edges
            edge_types (list[str] | Unset):
            max_steps (int | Unset): Graph-only maximum moves after the start node (default 8). The enclosing agent's
                iteration and tool limits also apply.
            neighbor_limit (int | Unset): Graph-only maximum candidate neighbors per node (default 8), further limited by
                the context budget.
            instruction (str | Unset): Optional caller-supplied workflow instruction retained in agent history.
            instruction_field (str | Unset): Explicitly opt in to following instructions from this top-level string
                field of each visited document. Instructions accumulate in agent history.
                Other document fields and unvisited neighbors remain untrusted evidence.
                The field must be included if the query uses a fields projection.
    """

    query_index: int
    strategy: RetrievalNavigationStrategy
    selection: RetrievalNavigationSelection
    index: str
    max_depth: int | Unset = UNSET
    beam_width: int | Unset = UNSET
    start_nodes: str | Unset = UNSET
    start_key: str | Unset = UNSET
    direction: EdgeDirection | Unset = UNSET
    edge_types: list[str] | Unset = UNSET
    max_steps: int | Unset = UNSET
    neighbor_limit: int | Unset = UNSET
    instruction: str | Unset = UNSET
    instruction_field: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        query_index = self.query_index

        strategy = self.strategy.value

        selection = self.selection.value

        index = self.index

        max_depth = self.max_depth

        beam_width = self.beam_width

        start_nodes = self.start_nodes

        start_key = self.start_key

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        edge_types: list[str] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        max_steps = self.max_steps

        neighbor_limit = self.neighbor_limit

        instruction = self.instruction

        instruction_field = self.instruction_field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "query_index": query_index,
                "strategy": strategy,
                "selection": selection,
                "index": index,
            }
        )
        if max_depth is not UNSET:
            field_dict["max_depth"] = max_depth
        if beam_width is not UNSET:
            field_dict["beam_width"] = beam_width
        if start_nodes is not UNSET:
            field_dict["start_nodes"] = start_nodes
        if start_key is not UNSET:
            field_dict["start_key"] = start_key
        if direction is not UNSET:
            field_dict["direction"] = direction
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if max_steps is not UNSET:
            field_dict["max_steps"] = max_steps
        if neighbor_limit is not UNSET:
            field_dict["neighbor_limit"] = neighbor_limit
        if instruction is not UNSET:
            field_dict["instruction"] = instruction
        if instruction_field is not UNSET:
            field_dict["instruction_field"] = instruction_field

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        query_index = d.pop("query_index")

        strategy = RetrievalNavigationStrategy(d.pop("strategy"))

        selection = RetrievalNavigationSelection(d.pop("selection"))

        index = d.pop("index")

        max_depth = d.pop("max_depth", UNSET)

        beam_width = d.pop("beam_width", UNSET)

        start_nodes = d.pop("start_nodes", UNSET)

        start_key = d.pop("start_key", UNSET)

        _direction = d.pop("direction", UNSET)
        direction: EdgeDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = EdgeDirection(_direction)

        edge_types = cast(list[str], d.pop("edge_types", UNSET))

        max_steps = d.pop("max_steps", UNSET)

        neighbor_limit = d.pop("neighbor_limit", UNSET)

        instruction = d.pop("instruction", UNSET)

        instruction_field = d.pop("instruction_field", UNSET)

        retrieval_navigation_config = cls(
            query_index=query_index,
            strategy=strategy,
            selection=selection,
            index=index,
            max_depth=max_depth,
            beam_width=beam_width,
            start_nodes=start_nodes,
            start_key=start_key,
            direction=direction,
            edge_types=edge_types,
            max_steps=max_steps,
            neighbor_limit=neighbor_limit,
            instruction=instruction,
            instruction_field=instruction_field,
        )

        return retrieval_navigation_config
