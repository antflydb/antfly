import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.edge_metadata import EdgeMetadata


T = TypeVar("T", bound="Edge")


@_attrs_define
class Edge:
    """A typed, weighted connection between documents

    Attributes:
        source (str): Base64-encoded source document key
        target (str): Base64-encoded target document key
        type_ (str): Edge type (e.g., "cites", "similar_to", "authored_by")
        weight (float): Edge weight/confidence (0.0 to 1.0)
        created_at (Union[Unset, datetime.datetime]): When the edge was created
        updated_at (Union[Unset, datetime.datetime]): When the edge was last updated
        metadata (Union[Unset, EdgeMetadata]): Optional edge metadata
    """

    source: str
    target: str
    type_: str
    weight: float
    created_at: Union[Unset, datetime.datetime] = UNSET
    updated_at: Union[Unset, datetime.datetime] = UNSET
    metadata: Union[Unset, "EdgeMetadata"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source = self.source

        target = self.target

        type_ = self.type_

        weight = self.weight

        created_at: Union[Unset, str] = UNSET
        if not isinstance(self.created_at, Unset):
            created_at = self.created_at.isoformat()

        updated_at: Union[Unset, str] = UNSET
        if not isinstance(self.updated_at, Unset):
            updated_at = self.updated_at.isoformat()

        metadata: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.metadata, Unset):
            metadata = self.metadata.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "source": source,
                "target": target,
                "type": type_,
                "weight": weight,
            }
        )
        if created_at is not UNSET:
            field_dict["created_at"] = created_at
        if updated_at is not UNSET:
            field_dict["updated_at"] = updated_at
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.edge_metadata import EdgeMetadata

        d = dict(src_dict)
        source = d.pop("source")

        target = d.pop("target")

        type_ = d.pop("type")

        weight = d.pop("weight")

        _created_at = d.pop("created_at", UNSET)
        created_at: Union[Unset, datetime.datetime]
        if isinstance(_created_at, Unset):
            created_at = UNSET
        else:
            created_at = isoparse(_created_at)

        _updated_at = d.pop("updated_at", UNSET)
        updated_at: Union[Unset, datetime.datetime]
        if isinstance(_updated_at, Unset):
            updated_at = UNSET
        else:
            updated_at = isoparse(_updated_at)

        _metadata = d.pop("metadata", UNSET)
        metadata: Union[Unset, EdgeMetadata]
        if isinstance(_metadata, Unset):
            metadata = UNSET
        else:
            metadata = EdgeMetadata.from_dict(_metadata)

        edge = cls(
            source=source,
            target=target,
            type_=type_,
            weight=weight,
            created_at=created_at,
            updated_at=updated_at,
            metadata=metadata,
        )

        edge.additional_properties = d
        return edge

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
