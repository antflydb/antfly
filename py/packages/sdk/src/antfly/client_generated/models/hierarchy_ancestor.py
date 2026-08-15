from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_ancestor_document import HierarchyAncestorDocument


T = TypeVar("T", bound="HierarchyAncestor")


@_attrs_define
class HierarchyAncestor:
    """
    Attributes:
        id (str | Unset):
        document (HierarchyAncestorDocument | Unset):
        key (str | Unset):
        artifact_name (str | Unset):
        source_field (str | Unset):
        provenance (Any | Unset):
    """

    id: str | Unset = UNSET
    document: HierarchyAncestorDocument | Unset = UNSET
    key: str | Unset = UNSET
    artifact_name: str | Unset = UNSET
    source_field: str | Unset = UNSET
    provenance: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.document, Unset):
            document = self.document.to_dict()

        key = self.key

        artifact_name = self.artifact_name

        source_field = self.source_field

        provenance = self.provenance

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if id is not UNSET:
            field_dict["id"] = id
        if document is not UNSET:
            field_dict["document"] = document
        if key is not UNSET:
            field_dict["key"] = key
        if artifact_name is not UNSET:
            field_dict["artifact_name"] = artifact_name
        if source_field is not UNSET:
            field_dict["source_field"] = source_field
        if provenance is not UNSET:
            field_dict["provenance"] = provenance

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_ancestor_document import HierarchyAncestorDocument

        d = dict(src_dict)
        id = d.pop("id", UNSET)

        _document = d.pop("document", UNSET)
        document: HierarchyAncestorDocument | Unset
        if isinstance(_document, Unset):
            document = UNSET
        else:
            document = HierarchyAncestorDocument.from_dict(_document)

        key = d.pop("key", UNSET)

        artifact_name = d.pop("artifact_name", UNSET)

        source_field = d.pop("source_field", UNSET)

        provenance = d.pop("provenance", UNSET)

        hierarchy_ancestor = cls(
            id=id,
            document=document,
            key=key,
            artifact_name=artifact_name,
            source_field=source_field,
            provenance=provenance,
        )

        hierarchy_ancestor.additional_properties = d
        return hierarchy_ancestor

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
