from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_document_match_all_filter_match_all import GraphDocumentMatchAllFilterMatchAll


T = TypeVar("T", bound="GraphDocumentMatchAllFilter")


@_attrs_define
class GraphDocumentMatchAllFilter:
    """
    Attributes:
        match_all (GraphDocumentMatchAllFilterMatchAll):
    """

    match_all: GraphDocumentMatchAllFilterMatchAll

    def to_dict(self) -> dict[str, Any]:
        match_all = self.match_all.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "match_all": match_all,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_document_match_all_filter_match_all import GraphDocumentMatchAllFilterMatchAll

        d = dict(src_dict)
        match_all = GraphDocumentMatchAllFilterMatchAll.from_dict(d.pop("match_all"))

        graph_document_match_all_filter = cls(
            match_all=match_all,
        )

        return graph_document_match_all_filter
