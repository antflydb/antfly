from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.path_edge import PathEdge


T = TypeVar("T", bound="Path")


@_attrs_define
class Path:
    """
    Attributes:
        nodes (Union[Unset, list[str]]): Ordered list of node keys (base64-encoded)
        edges (Union[Unset, list['PathEdge']]):
        total_weight (Union[Unset, float]):
        length (Union[Unset, int]):
    """

    nodes: Union[Unset, list[str]] = UNSET
    edges: Union[Unset, list["PathEdge"]] = UNSET
    total_weight: Union[Unset, float] = UNSET
    length: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        nodes: Union[Unset, list[str]] = UNSET
        if not isinstance(self.nodes, Unset):
            nodes = self.nodes

        edges: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.edges, Unset):
            edges = []
            for edges_item_data in self.edges:
                edges_item = edges_item_data.to_dict()
                edges.append(edges_item)

        total_weight = self.total_weight

        length = self.length

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if nodes is not UNSET:
            field_dict["nodes"] = nodes
        if edges is not UNSET:
            field_dict["edges"] = edges
        if total_weight is not UNSET:
            field_dict["total_weight"] = total_weight
        if length is not UNSET:
            field_dict["length"] = length

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.path_edge import PathEdge

        d = dict(src_dict)
        nodes = cast(list[str], d.pop("nodes", UNSET))

        edges = []
        _edges = d.pop("edges", UNSET)
        for edges_item_data in _edges or []:
            edges_item = PathEdge.from_dict(edges_item_data)

            edges.append(edges_item)

        total_weight = d.pop("total_weight", UNSET)

        length = d.pop("length", UNSET)

        path = cls(
            nodes=nodes,
            edges=edges,
            total_weight=total_weight,
            length=length,
        )

        path.additional_properties = d
        return path

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
