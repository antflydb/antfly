from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_offset_unit import ExtractionOffsetUnit
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_classification import ExtractionClassification
    from ..models.extraction_entity import ExtractionEntity
    from ..models.extraction_long_document_metadata import ExtractionLongDocumentMetadata
    from ..models.extraction_object_structure_metadata import ExtractionObjectStructureMetadata
    from ..models.extraction_object_structures import ExtractionObjectStructures
    from ..models.extraction_relation import ExtractionRelation
    from ..models.extraction_solver_diagnostics import ExtractionSolverDiagnostics


T = TypeVar("T", bound="ExtractionObject")


@_attrs_define
class ExtractionObject:
    """
    Attributes:
        id (str | Unset):
        offset_unit (ExtractionOffsetUnit | Unset): Half-open offsets into the immutable caller text. Version 2 defaults
            to utf8_bytes. No normalization, lowercasing or synthetic suffix is included in these coordinates.
        entities (list[ExtractionEntity] | Unset):
        relations (list[ExtractionRelation] | Unset):
        classifications (list[ExtractionClassification] | Unset):
        structures (ExtractionObjectStructures | Unset): Structure name to record array. Each record maps field names to
            value objects or arrays of value objects; v2 value objects follow ExtractionFieldValue.
        structure_metadata (ExtractionObjectStructureMetadata | Unset): Version 2 metadata arrays aligned with each
            named structure's record array.
        solvers (ExtractionSolverDiagnostics | Unset):
        long_document (ExtractionLongDocumentMetadata | Unset):
    """

    id: str | Unset = UNSET
    offset_unit: ExtractionOffsetUnit | Unset = UNSET
    entities: list[ExtractionEntity] | Unset = UNSET
    relations: list[ExtractionRelation] | Unset = UNSET
    classifications: list[ExtractionClassification] | Unset = UNSET
    structures: ExtractionObjectStructures | Unset = UNSET
    structure_metadata: ExtractionObjectStructureMetadata | Unset = UNSET
    solvers: ExtractionSolverDiagnostics | Unset = UNSET
    long_document: ExtractionLongDocumentMetadata | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        offset_unit: str | Unset = UNSET
        if not isinstance(self.offset_unit, Unset):
            offset_unit = self.offset_unit.value

        entities: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.entities, Unset):
            entities = []
            for entities_item_data in self.entities:
                entities_item = entities_item_data.to_dict()
                entities.append(entities_item)

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

        structure_metadata: dict[str, Any] | Unset = UNSET
        if not isinstance(self.structure_metadata, Unset):
            structure_metadata = self.structure_metadata.to_dict()

        solvers: dict[str, Any] | Unset = UNSET
        if not isinstance(self.solvers, Unset):
            solvers = self.solvers.to_dict()

        long_document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.long_document, Unset):
            long_document = self.long_document.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if id is not UNSET:
            field_dict["id"] = id
        if offset_unit is not UNSET:
            field_dict["offset_unit"] = offset_unit
        if entities is not UNSET:
            field_dict["entities"] = entities
        if relations is not UNSET:
            field_dict["relations"] = relations
        if classifications is not UNSET:
            field_dict["classifications"] = classifications
        if structures is not UNSET:
            field_dict["structures"] = structures
        if structure_metadata is not UNSET:
            field_dict["structure_metadata"] = structure_metadata
        if solvers is not UNSET:
            field_dict["solvers"] = solvers
        if long_document is not UNSET:
            field_dict["long_document"] = long_document

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_classification import ExtractionClassification
        from ..models.extraction_entity import ExtractionEntity
        from ..models.extraction_long_document_metadata import ExtractionLongDocumentMetadata
        from ..models.extraction_object_structure_metadata import ExtractionObjectStructureMetadata
        from ..models.extraction_object_structures import ExtractionObjectStructures
        from ..models.extraction_relation import ExtractionRelation
        from ..models.extraction_solver_diagnostics import ExtractionSolverDiagnostics

        d = dict(src_dict)
        id = d.pop("id", UNSET)

        _offset_unit = d.pop("offset_unit", UNSET)
        offset_unit: ExtractionOffsetUnit | Unset
        if isinstance(_offset_unit, Unset):
            offset_unit = UNSET
        else:
            offset_unit = ExtractionOffsetUnit(_offset_unit)

        _entities = d.pop("entities", UNSET)
        entities: list[ExtractionEntity] | Unset = UNSET
        if _entities is not UNSET:
            entities = []
            for entities_item_data in _entities:
                entities_item = ExtractionEntity.from_dict(entities_item_data)

                entities.append(entities_item)

        _relations = d.pop("relations", UNSET)
        relations: list[ExtractionRelation] | Unset = UNSET
        if _relations is not UNSET:
            relations = []
            for relations_item_data in _relations:
                relations_item = ExtractionRelation.from_dict(relations_item_data)

                relations.append(relations_item)

        _classifications = d.pop("classifications", UNSET)
        classifications: list[ExtractionClassification] | Unset = UNSET
        if _classifications is not UNSET:
            classifications = []
            for classifications_item_data in _classifications:
                classifications_item = ExtractionClassification.from_dict(classifications_item_data)

                classifications.append(classifications_item)

        _structures = d.pop("structures", UNSET)
        structures: ExtractionObjectStructures | Unset
        if isinstance(_structures, Unset):
            structures = UNSET
        else:
            structures = ExtractionObjectStructures.from_dict(_structures)

        _structure_metadata = d.pop("structure_metadata", UNSET)
        structure_metadata: ExtractionObjectStructureMetadata | Unset
        if isinstance(_structure_metadata, Unset):
            structure_metadata = UNSET
        else:
            structure_metadata = ExtractionObjectStructureMetadata.from_dict(_structure_metadata)

        _solvers = d.pop("solvers", UNSET)
        solvers: ExtractionSolverDiagnostics | Unset
        if isinstance(_solvers, Unset):
            solvers = UNSET
        else:
            solvers = ExtractionSolverDiagnostics.from_dict(_solvers)

        _long_document = d.pop("long_document", UNSET)
        long_document: ExtractionLongDocumentMetadata | Unset
        if isinstance(_long_document, Unset):
            long_document = UNSET
        else:
            long_document = ExtractionLongDocumentMetadata.from_dict(_long_document)

        extraction_object = cls(
            id=id,
            offset_unit=offset_unit,
            entities=entities,
            relations=relations,
            classifications=classifications,
            structures=structures,
            structure_metadata=structure_metadata,
            solvers=solvers,
            long_document=long_document,
        )

        extraction_object.additional_properties = d
        return extraction_object

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
