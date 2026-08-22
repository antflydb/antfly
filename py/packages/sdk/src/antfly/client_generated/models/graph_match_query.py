from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_aggregates_return import GraphAggregatesReturn
    from ..models.graph_bindings_return import GraphBindingsReturn
    from ..models.graph_match import GraphMatch


T = TypeVar("T", bound="GraphMatchQuery")


@_attrs_define
class GraphMatchQuery:
    """Conjunctive graph match over the complete authorized source universe. Top-level retrieval queries and filters do not
    scope that universe; put source constraints on the node named by match.anchor. Results are exact or the request
    fails; execution never labels a partial aggregate exact. Source anchors are streamed in stable snapshot-pinned
    pages; transient expansion state remains bounded, and execution observes request deadlines, cancellation, and server
    resource admission.

        Attributes:
            index (str):
            match (GraphMatch):
            return_ (GraphAggregatesReturn | GraphBindingsReturn): Return bindings or exact aggregates. Bindings and
                aggregates are mutually exclusive.
    """

    index: str
    match: GraphMatch
    return_: GraphAggregatesReturn | GraphBindingsReturn

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_bindings_return import GraphBindingsReturn

        index = self.index

        match = self.match.to_dict()

        return_: dict[str, Any]
        if isinstance(self.return_, GraphBindingsReturn):
            return_ = self.return_.to_dict()
        else:
            return_ = self.return_.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "index": index,
                "match": match,
                "return": return_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_aggregates_return import GraphAggregatesReturn
        from ..models.graph_bindings_return import GraphBindingsReturn
        from ..models.graph_match import GraphMatch

        d = dict(src_dict)
        index = d.pop("index")

        match = GraphMatch.from_dict(d.pop("match"))

        def _parse_return_(data: object) -> GraphAggregatesReturn | GraphBindingsReturn:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_return_type_0 = GraphBindingsReturn.from_dict(data)

                return componentsschemas_graph_return_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_graph_return_type_1 = GraphAggregatesReturn.from_dict(data)

            return componentsschemas_graph_return_type_1

        return_ = _parse_return_(d.pop("return"))

        graph_match_query = cls(
            index=index,
            match=match,
            return_=return_,
        )

        return graph_match_query
