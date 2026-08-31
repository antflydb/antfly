from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_document_term_range_body import GraphDocumentTermRangeBody


T = TypeVar("T", bound="GraphDocumentTermRangeFilter")


@_attrs_define
class GraphDocumentTermRangeFilter:
    """
    Attributes:
        term_range (GraphDocumentTermRangeBody): At least one of min or max is required and enforced by every Antfly
            execution boundary.
    """

    term_range: GraphDocumentTermRangeBody

    def to_dict(self) -> dict[str, Any]:
        term_range = self.term_range.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "term_range": term_range,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_document_term_range_body import GraphDocumentTermRangeBody

        d = dict(src_dict)
        term_range = GraphDocumentTermRangeBody.from_dict(d.pop("term_range"))

        graph_document_term_range_filter = cls(
            term_range=term_range,
        )

        return graph_document_term_range_filter
