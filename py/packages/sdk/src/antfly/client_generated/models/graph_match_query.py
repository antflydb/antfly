from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_match import GraphMatch


T = TypeVar("T", bound="GraphMatchQuery")


@_attrs_define
class GraphMatchQuery:
    """
    Attributes:
        index (str):
        match (GraphMatch):
        return_ (Any): Return bindings or exact aggregates. Bindings and aggregates are mutually exclusive.
    """

    index: str
    match: GraphMatch
    return_: Any

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        match = self.match.to_dict()

        return_: Any
        return_ = self.return_

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
        from ..models.graph_match import GraphMatch

        d = dict(src_dict)
        index = d.pop("index")

        match = GraphMatch.from_dict(d.pop("match"))

        def _parse_return_(data: object) -> Any:
            return cast(Any, data)

        return_ = _parse_return_(d.pop("return"))

        graph_match_query = cls(
            index=index,
            match=match,
            return_=return_,
        )

        return graph_match_query
