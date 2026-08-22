from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_document_query import GraphDocumentQuery


T = TypeVar("T", bound="GraphMatchNode")


@_attrs_define
class GraphMatchNode:
    """
    Attributes:
        filter_ (GraphDocumentQuery | Unset): A document-query expression in either public QueryRequest.filter_query
            syntax or canonical Antfly filter AST syntax. Graph queries embed this existing document query language; alias-
            to-alias predicates belong in GraphMatch.where.
    """

    filter_: GraphDocumentQuery | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        filter_: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filter_, Unset):
            filter_ = self.filter_.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if filter_ is not UNSET:
            field_dict["filter"] = filter_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_document_query import GraphDocumentQuery

        d = dict(src_dict)
        _filter_ = d.pop("filter", UNSET)
        filter_: GraphDocumentQuery | Unset
        if isinstance(_filter_, Unset):
            filter_ = UNSET
        else:
            filter_ = GraphDocumentQuery.from_dict(_filter_)

        graph_match_node = cls(
            filter_=filter_,
        )

        return graph_match_node
