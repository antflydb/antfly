from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.path_edge_metadata import PathEdgeMetadata


T = TypeVar("T", bound="PathEdge")


@_attrs_define
class PathEdge:
    """
    Attributes:
        source (Union[Unset, str]):
        target (Union[Unset, str]):
        type_ (Union[Unset, str]):
        weight (Union[Unset, float]):
        metadata (Union[Unset, PathEdgeMetadata]):
    """

    source: Union[Unset, str] = UNSET
    target: Union[Unset, str] = UNSET
    type_: Union[Unset, str] = UNSET
    weight: Union[Unset, float] = UNSET
    metadata: Union[Unset, "PathEdgeMetadata"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source = self.source

        target = self.target

        type_ = self.type_

        weight = self.weight

        metadata: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.metadata, Unset):
            metadata = self.metadata.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if source is not UNSET:
            field_dict["source"] = source
        if target is not UNSET:
            field_dict["target"] = target
        if type_ is not UNSET:
            field_dict["type"] = type_
        if weight is not UNSET:
            field_dict["weight"] = weight
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.path_edge_metadata import PathEdgeMetadata

        d = dict(src_dict)
        source = d.pop("source", UNSET)

        target = d.pop("target", UNSET)

        type_ = d.pop("type", UNSET)

        weight = d.pop("weight", UNSET)

        _metadata = d.pop("metadata", UNSET)
        metadata: Union[Unset, PathEdgeMetadata]
        if isinstance(_metadata, Unset):
            metadata = UNSET
        else:
            metadata = PathEdgeMetadata.from_dict(_metadata)

        path_edge = cls(
            source=source,
            target=target,
            type_=type_,
            weight=weight,
            metadata=metadata,
        )

        path_edge.additional_properties = d
        return path_edge

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
