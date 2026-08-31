from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_document_bool_field_body import GraphDocumentBoolFieldBody


T = TypeVar("T", bound="GraphDocumentBoolFieldFilter")


@_attrs_define
class GraphDocumentBoolFieldFilter:
    """
    Attributes:
        bool_field (GraphDocumentBoolFieldBody):
    """

    bool_field: GraphDocumentBoolFieldBody

    def to_dict(self) -> dict[str, Any]:
        bool_field = self.bool_field.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "bool_field": bool_field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_document_bool_field_body import GraphDocumentBoolFieldBody

        d = dict(src_dict)
        bool_field = GraphDocumentBoolFieldBody.from_dict(d.pop("bool_field"))

        graph_document_bool_field_filter = cls(
            bool_field=bool_field,
        )

        return graph_document_bool_field_filter
