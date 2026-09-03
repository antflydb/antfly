from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_path import GraphPath
    from ..models.graph_path_result_document import GraphPathResultDocument


T = TypeVar("T", bound="GraphPathResult")


@_attrs_define
class GraphPathResult:
    """One authoritative pathfinding result. The terminal identity is the last element of path.nodes, so it is never
    duplicated in a parallel node array.

        Attributes:
            path (GraphPath): An ordered canonical graph path with table-qualified node identities and a self-describing
                ranking score.
            document (GraphPathResultDocument | Unset): Stored terminal document when include_documents=true and the
                terminal identity exists at the pinned snapshot; otherwise omitted.
    """

    path: GraphPath
    document: GraphPathResultDocument | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        path = self.path.to_dict()

        document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.document, Unset):
            document = self.document.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "path": path,
            }
        )
        if document is not UNSET:
            field_dict["document"] = document

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_path import GraphPath
        from ..models.graph_path_result_document import GraphPathResultDocument

        d = dict(src_dict)
        path = GraphPath.from_dict(d.pop("path"))

        _document = d.pop("document", UNSET)
        document: GraphPathResultDocument | Unset
        if isinstance(_document, Unset):
            document = UNSET
        else:
            document = GraphPathResultDocument.from_dict(_document)

        graph_path_result = cls(
            path=path,
            document=document,
        )

        return graph_path_result
