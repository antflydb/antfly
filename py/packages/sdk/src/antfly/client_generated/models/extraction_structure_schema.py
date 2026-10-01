from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_structure_schema_mode import ExtractionStructureSchemaMode
from ..models.extraction_structure_schema_occurrence_policy import ExtractionStructureSchemaOccurrencePolicy
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_structure_schema_fields import ExtractionStructureSchemaFields


T = TypeVar("T", bound="ExtractionStructureSchema")


@_attrs_define
class ExtractionStructureSchema:
    """
    Attributes:
        fields (ExtractionStructureSchemaFields):
        mode (ExtractionStructureSchemaMode | Unset): Version 2 record grouping. Omission preserves one-record
            extraction.
        anchor (str | Unset): Natural mode only; defaults to the first declared field.
        occurrence_policy (ExtractionStructureSchemaOccurrencePolicy | Unset):
    """

    fields: ExtractionStructureSchemaFields
    mode: ExtractionStructureSchemaMode | Unset = UNSET
    anchor: str | Unset = UNSET
    occurrence_policy: ExtractionStructureSchemaOccurrencePolicy | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        fields = self.fields.to_dict()

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        anchor = self.anchor

        occurrence_policy: str | Unset = UNSET
        if not isinstance(self.occurrence_policy, Unset):
            occurrence_policy = self.occurrence_policy.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "fields": fields,
            }
        )
        if mode is not UNSET:
            field_dict["mode"] = mode
        if anchor is not UNSET:
            field_dict["anchor"] = anchor
        if occurrence_policy is not UNSET:
            field_dict["occurrence_policy"] = occurrence_policy

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_structure_schema_fields import ExtractionStructureSchemaFields

        d = dict(src_dict)
        fields = ExtractionStructureSchemaFields.from_dict(d.pop("fields"))

        _mode = d.pop("mode", UNSET)
        mode: ExtractionStructureSchemaMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = ExtractionStructureSchemaMode(_mode)

        anchor = d.pop("anchor", UNSET)

        _occurrence_policy = d.pop("occurrence_policy", UNSET)
        occurrence_policy: ExtractionStructureSchemaOccurrencePolicy | Unset
        if isinstance(_occurrence_policy, Unset):
            occurrence_policy = UNSET
        else:
            occurrence_policy = ExtractionStructureSchemaOccurrencePolicy(_occurrence_policy)

        extraction_structure_schema = cls(
            fields=fields,
            mode=mode,
            anchor=anchor,
            occurrence_policy=occurrence_policy,
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
