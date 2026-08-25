from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_binding_node_document import GraphBindingNodeDocument


T = TypeVar("T", bound="GraphBindingNode")


@_attrs_define
class GraphBindingNode:
    """One exact node identity projected from a MATCH binding. Conjunctive bindings deliberately do not expose traversal
    depth, distance, or path: those values are not uniquely defined for branched patterns and may depend on execution
    order.

        Attributes:
            key (str): Exact document key.
            table (str | Unset): Owning table for a cross-table binding; omitted for the queried table.
            document (GraphBindingNodeDocument | Unset): Stored document when include_documents=true.
    """

    key: str
    table: str | Unset = UNSET
    document: GraphBindingNodeDocument | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        table = self.table

        document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.document, Unset):
            document = self.document.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "key": key,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table
        if document is not UNSET:
            field_dict["document"] = document

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_binding_node_document import GraphBindingNodeDocument

        d = dict(src_dict)
        key = d.pop("key")

        table = d.pop("table", UNSET)

        _document = d.pop("document", UNSET)
        document: GraphBindingNodeDocument | Unset
        if isinstance(_document, Unset):
            document = UNSET
        else:
            document = GraphBindingNodeDocument.from_dict(_document)

        graph_binding_node = cls(
            key=key,
            table=table,
            document=document,
        )

        return graph_binding_node
