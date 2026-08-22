from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_document_numeric_range_body import GraphDocumentNumericRangeBody


T = TypeVar("T", bound="GraphDocumentNumericRangeFilter")


@_attrs_define
class GraphDocumentNumericRangeFilter:
    """
    Attributes:
        numeric_range (GraphDocumentNumericRangeBody): At least one of min or max is required and enforced by the
            server.
    """

    numeric_range: GraphDocumentNumericRangeBody

    def to_dict(self) -> dict[str, Any]:
        numeric_range = self.numeric_range.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "numeric_range": numeric_range,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_document_numeric_range_body import GraphDocumentNumericRangeBody

        d = dict(src_dict)
        numeric_range = GraphDocumentNumericRangeBody.from_dict(d.pop("numeric_range"))

        graph_document_numeric_range_filter = cls(
            numeric_range=numeric_range,
        )

        return graph_document_numeric_range_filter
