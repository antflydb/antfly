from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_document_match_none_filter_match_none import GraphDocumentMatchNoneFilterMatchNone


T = TypeVar("T", bound="GraphDocumentMatchNoneFilter")


@_attrs_define
class GraphDocumentMatchNoneFilter:
    """
    Attributes:
        match_none (GraphDocumentMatchNoneFilterMatchNone):
    """

    match_none: GraphDocumentMatchNoneFilterMatchNone

    def to_dict(self) -> dict[str, Any]:
        match_none = self.match_none.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "match_none": match_none,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_document_match_none_filter_match_none import GraphDocumentMatchNoneFilterMatchNone

        d = dict(src_dict)
        match_none = GraphDocumentMatchNoneFilterMatchNone.from_dict(d.pop("match_none"))

        graph_document_match_none_filter = cls(
            match_none=match_none,
        )

        return graph_document_match_none_filter
