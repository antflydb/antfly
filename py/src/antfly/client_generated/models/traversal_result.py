from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.edge import Edge
    from ..models.traversal_result_document import TraversalResultDocument


T = TypeVar("T", bound="TraversalResult")


@_attrs_define
class TraversalResult:
    """A single result from graph traversal

    Attributes:
        key (str): Base64-encoded document key
        depth (int): Distance from start node (0 = start node)
        document (Union[Unset, TraversalResultDocument]): Document data (if loaded)
        path (Union[Unset, list[str]]): Sequence of keys from start to this node (if include_paths=true)
        path_edges (Union[Unset, list['Edge']]): Sequence of edges from start to this node (if include_paths=true)
        total_weight (Union[Unset, float]): Product of edge weights along the path
    """

    key: str
    depth: int
    document: Union[Unset, "TraversalResultDocument"] = UNSET
    path: Union[Unset, list[str]] = UNSET
    path_edges: Union[Unset, list["Edge"]] = UNSET
    total_weight: Union[Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        depth = self.depth

        document: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.document, Unset):
            document = self.document.to_dict()

        path: Union[Unset, list[str]] = UNSET
        if not isinstance(self.path, Unset):
            path = self.path

        path_edges: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.path_edges, Unset):
            path_edges = []
            for path_edges_item_data in self.path_edges:
                path_edges_item = path_edges_item_data.to_dict()
                path_edges.append(path_edges_item)

        total_weight = self.total_weight

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key": key,
                "depth": depth,
            }
        )
        if document is not UNSET:
            field_dict["document"] = document
        if path is not UNSET:
            field_dict["path"] = path
        if path_edges is not UNSET:
            field_dict["path_edges"] = path_edges
        if total_weight is not UNSET:
            field_dict["total_weight"] = total_weight

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.edge import Edge
        from ..models.traversal_result_document import TraversalResultDocument

        d = dict(src_dict)
        key = d.pop("key")

        depth = d.pop("depth")

        _document = d.pop("document", UNSET)
        document: Union[Unset, TraversalResultDocument]
        if isinstance(_document, Unset):
            document = UNSET
        else:
            document = TraversalResultDocument.from_dict(_document)

        path = cast(list[str], d.pop("path", UNSET))

        path_edges = []
        _path_edges = d.pop("path_edges", UNSET)
        for path_edges_item_data in _path_edges or []:
            path_edges_item = Edge.from_dict(path_edges_item_data)

            path_edges.append(path_edges_item)

        total_weight = d.pop("total_weight", UNSET)

        traversal_result = cls(
            key=key,
            depth=depth,
            document=document,
            path=path,
            path_edges=path_edges,
            total_weight=total_weight,
        )

        traversal_result.additional_properties = d
        return traversal_result

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
