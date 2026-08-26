from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_document_date_range_body import GraphDocumentDateRangeBody


T = TypeVar("T", bound="GraphDocumentDateRangeFilter")


@_attrs_define
class GraphDocumentDateRangeFilter:
    """
    Attributes:
        date_range (GraphDocumentDateRangeBody): At least one of start or end is required and enforced by every Antfly
            execution boundary. Bounds are RFC 3339 instants in Antfly's unsigned Unix-nanosecond domain, from
            1970-01-01T00:00:00Z through 2554-07-21T23:34:33.709551615Z inclusive.
    """

    date_range: GraphDocumentDateRangeBody

    def to_dict(self) -> dict[str, Any]:
        date_range = self.date_range.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "date_range": date_range,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_document_date_range_body import GraphDocumentDateRangeBody

        d = dict(src_dict)
        date_range = GraphDocumentDateRangeBody.from_dict(d.pop("date_range"))

        graph_document_date_range_filter = cls(
            date_range=date_range,
        )

        return graph_document_date_range_filter
