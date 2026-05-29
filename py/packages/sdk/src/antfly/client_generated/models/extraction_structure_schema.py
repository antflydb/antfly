from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_structure_schema_fields import ExtractionStructureSchemaFields


T = TypeVar("T", bound="ExtractionStructureSchema")


@_attrs_define
class ExtractionStructureSchema:
    """
    Attributes:
        fields (ExtractionStructureSchemaFields | Unset):
    """

    fields: ExtractionStructureSchemaFields | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        fields: dict[str, Any] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if fields is not UNSET:
            field_dict["fields"] = fields

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_structure_schema_fields import ExtractionStructureSchemaFields

        d = dict(src_dict)
        _fields = d.pop("fields", UNSET)
        fields: ExtractionStructureSchemaFields | Unset
        if isinstance(_fields, Unset):
            fields = UNSET
        else:
            fields = ExtractionStructureSchemaFields.from_dict(_fields)

        extraction_structure_schema = cls(
            fields=fields,
        )

        extraction_structure_schema.additional_properties = d
        return extraction_structure_schema

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
