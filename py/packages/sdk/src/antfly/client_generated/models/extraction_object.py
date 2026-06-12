from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_classification import ExtractionClassification
    from ..models.extraction_entity import ExtractionEntity
    from ..models.extraction_object_structures import ExtractionObjectStructures
    from ..models.extraction_relation import ExtractionRelation


T = TypeVar("T", bound="ExtractionObject")


@_attrs_define
class ExtractionObject:
    """
    Attributes:
        id (str | Unset):
        entities (list[ExtractionEntity] | Unset):
        relations (list[ExtractionRelation] | Unset):
        classifications (list[ExtractionClassification] | Unset):
        structures (ExtractionObjectStructures | Unset):
    """

    id: str | Unset = UNSET
    entities: list[ExtractionEntity] | Unset = UNSET
    relations: list[ExtractionRelation] | Unset = UNSET
    classifications: list[ExtractionClassification] | Unset = UNSET
    structures: ExtractionObjectStructures | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

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

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if id is not UNSET:
            field_dict["id"] = id
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
        from ..models.extraction_classification import ExtractionClassification
        from ..models.extraction_entity import ExtractionEntity
        from ..models.extraction_object_structures import ExtractionObjectStructures
        from ..models.extraction_relation import ExtractionRelation

        d = dict(src_dict)
        id = d.pop("id", UNSET)

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

        extraction_object = cls(
            id=id,
            entities=entities,
            relations=relations,
            classifications=classifications,
            structures=structures,
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
