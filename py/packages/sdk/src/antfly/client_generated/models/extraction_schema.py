from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_classification_schema import ExtractionClassificationSchema
    from ..models.extraction_relation_schema import ExtractionRelationSchema
    from ..models.extraction_schema_structures import ExtractionSchemaStructures


T = TypeVar("T", bound="ExtractionSchema")


@_attrs_define
class ExtractionSchema:
    """
    Attributes:
        entities (list[str] | Unset):
        relations (list[ExtractionRelationSchema] | Unset):
        classifications (list[ExtractionClassificationSchema] | Unset):
        structures (ExtractionSchemaStructures | Unset):
    """

    entities: list[str] | Unset = UNSET
    relations: list[ExtractionRelationSchema] | Unset = UNSET
    classifications: list[ExtractionClassificationSchema] | Unset = UNSET
    structures: ExtractionSchemaStructures | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        entities: list[str] | Unset = UNSET
        if not isinstance(self.entities, Unset):
            entities = self.entities

        relations: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.relations, Unset):
            relations = []
            for relations_item_data in self.relations:
                relations_item = relations_item_data.to_dict()
                relations.append(relations_item)

        classifications: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.classifications, Unset):
            classifications = []
            for classifications_item_data in self.classifications:
                classifications_item = classifications_item_data.to_dict()
                classifications.append(classifications_item)

        structures: dict[str, Any] | Unset = UNSET
        if not isinstance(self.structures, Unset):
            structures = self.structures.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if entities is not UNSET:
            field_dict["entities"] = entities
        if relations is not UNSET:
            field_dict["relations"] = relations
        if classifications is not UNSET:
            field_dict["classifications"] = classifications
        if structures is not UNSET:
            field_dict["structures"] = structures

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_classification_schema import ExtractionClassificationSchema
        from ..models.extraction_relation_schema import ExtractionRelationSchema
        from ..models.extraction_schema_structures import ExtractionSchemaStructures

        d = dict(src_dict)
        entities = cast(list[str], d.pop("entities", UNSET))

        _relations = d.pop("relations", UNSET)
        relations: list[ExtractionRelationSchema] | Unset = UNSET
        if _relations is not UNSET:
            relations = []
            for relations_item_data in _relations:
                relations_item = ExtractionRelationSchema.from_dict(relations_item_data)

                relations.append(relations_item)

        _classifications = d.pop("classifications", UNSET)
        classifications: list[ExtractionClassificationSchema] | Unset = UNSET
        if _classifications is not UNSET:
            classifications = []
            for classifications_item_data in _classifications:
                classifications_item = ExtractionClassificationSchema.from_dict(classifications_item_data)

                classifications.append(classifications_item)

        _structures = d.pop("structures", UNSET)
        structures: ExtractionSchemaStructures | Unset
        if isinstance(_structures, Unset):
            structures = UNSET
        else:
            structures = ExtractionSchemaStructures.from_dict(_structures)

        extraction_schema = cls(
            entities=entities,
            relations=relations,
            classifications=classifications,
            structures=structures,
        )

        extraction_schema.additional_properties = d
        return extraction_schema

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
