from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_classification_schema import ExtractionClassificationSchema
    from ..models.extraction_constraint_and import ExtractionConstraintAnd
    from ..models.extraction_constraint_any_other_selected import ExtractionConstraintAnyOtherSelected
    from ..models.extraction_constraint_any_selected import ExtractionConstraintAnySelected
    from ..models.extraction_constraint_at_level import ExtractionConstraintAtLevel
    from ..models.extraction_constraint_cardinality import ExtractionConstraintCardinality
    from ..models.extraction_constraint_exactly_one_of import ExtractionConstraintExactlyOneOf
    from ..models.extraction_constraint_excludes import ExtractionConstraintExcludes
    from ..models.extraction_constraint_iff import ExtractionConstraintIff
    from ..models.extraction_constraint_implies import ExtractionConstraintImplies
    from ..models.extraction_constraint_is_default import ExtractionConstraintIsDefault
    from ..models.extraction_constraint_label_ref import ExtractionConstraintLabelRef
    from ..models.extraction_constraint_max_level import ExtractionConstraintMaxLevel
    from ..models.extraction_constraint_min_level import ExtractionConstraintMinLevel
    from ..models.extraction_constraint_not import ExtractionConstraintNot
    from ..models.extraction_constraint_or import ExtractionConstraintOr
    from ..models.extraction_joint_schema import ExtractionJointSchema
    from ..models.extraction_relation_schema import ExtractionRelationSchema
    from ..models.extraction_schema_entity_attributes import ExtractionSchemaEntityAttributes
    from ..models.extraction_schema_entity_definitions import ExtractionSchemaEntityDefinitions
    from ..models.extraction_schema_structures import ExtractionSchemaStructures


T = TypeVar("T", bound="ExtractionSchema")


@_attrs_define
class ExtractionSchema:
    """Version 1 selects one extraction family; entities may accompany relations.
    With schema_version 2, entities, attributes, classifications, structures,
    and ordinary relations may share one encoded input. joint_ie is a separate,
    mutually exclusive typed graph schema. The version 2 compiler rejects
    unknown fields and validates all references before model execution.

        Attributes:
            entities (list[str] | Unset):
            relations (list[ExtractionRelationSchema] | Unset):
            classifications (list[ExtractionClassificationSchema] | Unset):
            structures (ExtractionSchemaStructures | Unset):
            entity_definitions (ExtractionSchemaEntityDefinitions | Unset):
            entity_attributes (ExtractionSchemaEntityAttributes | Unset):
            classification_constraints (list[ExtractionConstraintAnd | ExtractionConstraintAnyOtherSelected |
                ExtractionConstraintAnySelected | ExtractionConstraintAtLevel | ExtractionConstraintCardinality |
                ExtractionConstraintExactlyOneOf | ExtractionConstraintExcludes | ExtractionConstraintIff |
                ExtractionConstraintImplies | ExtractionConstraintIsDefault | ExtractionConstraintLabelRef |
                ExtractionConstraintMaxLevel | ExtractionConstraintMinLevel | ExtractionConstraintNot | ExtractionConstraintOr]
                | Unset):
            joint_ie (ExtractionJointSchema | Unset): Separate typed graph schema, mutually exclusive with ordinary
                extraction families. Hard typed endpoints, overlap, uniqueness and declared graph constraints apply to every
                returned edge, including derived companions.
    """

    entities: list[str] | Unset = UNSET
    relations: list[ExtractionRelationSchema] | Unset = UNSET
    classifications: list[ExtractionClassificationSchema] | Unset = UNSET
    structures: ExtractionSchemaStructures | Unset = UNSET
    entity_definitions: ExtractionSchemaEntityDefinitions | Unset = UNSET
    entity_attributes: ExtractionSchemaEntityAttributes | Unset = UNSET
    classification_constraints: (
        list[
            ExtractionConstraintAnd
            | ExtractionConstraintAnyOtherSelected
            | ExtractionConstraintAnySelected
            | ExtractionConstraintAtLevel
            | ExtractionConstraintCardinality
            | ExtractionConstraintExactlyOneOf
            | ExtractionConstraintExcludes
            | ExtractionConstraintIff
            | ExtractionConstraintImplies
            | ExtractionConstraintIsDefault
            | ExtractionConstraintLabelRef
            | ExtractionConstraintMaxLevel
            | ExtractionConstraintMinLevel
            | ExtractionConstraintNot
            | ExtractionConstraintOr
        ]
        | Unset
    ) = UNSET
    joint_ie: ExtractionJointSchema | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.extraction_constraint_and import ExtractionConstraintAnd
        from ..models.extraction_constraint_any_other_selected import ExtractionConstraintAnyOtherSelected
        from ..models.extraction_constraint_any_selected import ExtractionConstraintAnySelected
        from ..models.extraction_constraint_at_level import ExtractionConstraintAtLevel
        from ..models.extraction_constraint_cardinality import ExtractionConstraintCardinality
        from ..models.extraction_constraint_exactly_one_of import ExtractionConstraintExactlyOneOf
        from ..models.extraction_constraint_iff import ExtractionConstraintIff
        from ..models.extraction_constraint_implies import ExtractionConstraintImplies
        from ..models.extraction_constraint_is_default import ExtractionConstraintIsDefault
        from ..models.extraction_constraint_label_ref import ExtractionConstraintLabelRef
        from ..models.extraction_constraint_max_level import ExtractionConstraintMaxLevel
        from ..models.extraction_constraint_min_level import ExtractionConstraintMinLevel
        from ..models.extraction_constraint_not import ExtractionConstraintNot
        from ..models.extraction_constraint_or import ExtractionConstraintOr

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

        entity_definitions: dict[str, Any] | Unset = UNSET
        if not isinstance(self.entity_definitions, Unset):
            entity_definitions = self.entity_definitions.to_dict()

        entity_attributes: dict[str, Any] | Unset = UNSET
        if not isinstance(self.entity_attributes, Unset):
            entity_attributes = self.entity_attributes.to_dict()

        classification_constraints: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.classification_constraints, Unset):
            classification_constraints = []
            for classification_constraints_item_data in self.classification_constraints:
                classification_constraints_item: dict[str, Any]
                if isinstance(classification_constraints_item_data, ExtractionConstraintLabelRef):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintAnySelected):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintAnyOtherSelected):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintIsDefault):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintCardinality):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintMinLevel):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintMaxLevel):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintAtLevel):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintNot):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintAnd):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintOr):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintExactlyOneOf):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintImplies):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                elif isinstance(classification_constraints_item_data, ExtractionConstraintIff):
                    classification_constraints_item = classification_constraints_item_data.to_dict()
                else:
                    classification_constraints_item = classification_constraints_item_data.to_dict()

                classification_constraints.append(classification_constraints_item)

        joint_ie: dict[str, Any] | Unset = UNSET
        if not isinstance(self.joint_ie, Unset):
            joint_ie = self.joint_ie.to_dict()

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
        if entity_definitions is not UNSET:
            field_dict["entity_definitions"] = entity_definitions
        if entity_attributes is not UNSET:
            field_dict["entity_attributes"] = entity_attributes
        if classification_constraints is not UNSET:
            field_dict["classification_constraints"] = classification_constraints
        if joint_ie is not UNSET:
            field_dict["joint_ie"] = joint_ie

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_classification_schema import ExtractionClassificationSchema
        from ..models.extraction_constraint_and import ExtractionConstraintAnd
        from ..models.extraction_constraint_any_other_selected import ExtractionConstraintAnyOtherSelected
        from ..models.extraction_constraint_any_selected import ExtractionConstraintAnySelected
        from ..models.extraction_constraint_at_level import ExtractionConstraintAtLevel
        from ..models.extraction_constraint_cardinality import ExtractionConstraintCardinality
        from ..models.extraction_constraint_exactly_one_of import ExtractionConstraintExactlyOneOf
        from ..models.extraction_constraint_excludes import ExtractionConstraintExcludes
        from ..models.extraction_constraint_iff import ExtractionConstraintIff
        from ..models.extraction_constraint_implies import ExtractionConstraintImplies
        from ..models.extraction_constraint_is_default import ExtractionConstraintIsDefault
        from ..models.extraction_constraint_label_ref import ExtractionConstraintLabelRef
        from ..models.extraction_constraint_max_level import ExtractionConstraintMaxLevel
        from ..models.extraction_constraint_min_level import ExtractionConstraintMinLevel
        from ..models.extraction_constraint_not import ExtractionConstraintNot
        from ..models.extraction_constraint_or import ExtractionConstraintOr
        from ..models.extraction_joint_schema import ExtractionJointSchema
        from ..models.extraction_relation_schema import ExtractionRelationSchema
        from ..models.extraction_schema_entity_attributes import ExtractionSchemaEntityAttributes
        from ..models.extraction_schema_entity_definitions import ExtractionSchemaEntityDefinitions
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

        _entity_definitions = d.pop("entity_definitions", UNSET)
        entity_definitions: ExtractionSchemaEntityDefinitions | Unset
        if isinstance(_entity_definitions, Unset):
            entity_definitions = UNSET
        else:
            entity_definitions = ExtractionSchemaEntityDefinitions.from_dict(_entity_definitions)

        _entity_attributes = d.pop("entity_attributes", UNSET)
        entity_attributes: ExtractionSchemaEntityAttributes | Unset
        if isinstance(_entity_attributes, Unset):
            entity_attributes = UNSET
        else:
            entity_attributes = ExtractionSchemaEntityAttributes.from_dict(_entity_attributes)

        _classification_constraints = d.pop("classification_constraints", UNSET)
        classification_constraints: (
            list[
                ExtractionConstraintAnd
                | ExtractionConstraintAnyOtherSelected
                | ExtractionConstraintAnySelected
                | ExtractionConstraintAtLevel
                | ExtractionConstraintCardinality
                | ExtractionConstraintExactlyOneOf
                | ExtractionConstraintExcludes
                | ExtractionConstraintIff
                | ExtractionConstraintImplies
                | ExtractionConstraintIsDefault
                | ExtractionConstraintLabelRef
                | ExtractionConstraintMaxLevel
                | ExtractionConstraintMinLevel
                | ExtractionConstraintNot
                | ExtractionConstraintOr
            ]
            | Unset
        ) = UNSET
        if _classification_constraints is not UNSET:
            classification_constraints = []
            for classification_constraints_item_data in _classification_constraints:

                def _parse_classification_constraints_item(
                    data: object,
                ) -> (
                    ExtractionConstraintAnd
                    | ExtractionConstraintAnyOtherSelected
                    | ExtractionConstraintAnySelected
                    | ExtractionConstraintAtLevel
                    | ExtractionConstraintCardinality
                    | ExtractionConstraintExactlyOneOf
                    | ExtractionConstraintExcludes
                    | ExtractionConstraintIff
                    | ExtractionConstraintImplies
                    | ExtractionConstraintIsDefault
                    | ExtractionConstraintLabelRef
                    | ExtractionConstraintMaxLevel
                    | ExtractionConstraintMinLevel
                    | ExtractionConstraintNot
                    | ExtractionConstraintOr
                ):
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_0 = (
                            ExtractionConstraintLabelRef.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_1 = (
                            ExtractionConstraintAnySelected.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_1
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_2 = (
                            ExtractionConstraintAnyOtherSelected.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_2
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_3 = (
                            ExtractionConstraintIsDefault.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_3
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_4 = (
                            ExtractionConstraintCardinality.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_4
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_5 = (
                            ExtractionConstraintMinLevel.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_5
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_6 = (
                            ExtractionConstraintMaxLevel.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_6
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_7 = (
                            ExtractionConstraintAtLevel.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_7
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_8 = (
                            ExtractionConstraintNot.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_8
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_9 = (
                            ExtractionConstraintAnd.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_9
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_10 = (
                            ExtractionConstraintOr.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_10
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_11 = (
                            ExtractionConstraintExactlyOneOf.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_11
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_12 = (
                            ExtractionConstraintImplies.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_12
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_constraint_type_13 = (
                            ExtractionConstraintIff.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_constraint_type_13
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_14 = (
                        ExtractionConstraintExcludes.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_14

                classification_constraints_item = _parse_classification_constraints_item(
                    classification_constraints_item_data
                )

                classification_constraints.append(classification_constraints_item)

        _joint_ie = d.pop("joint_ie", UNSET)
        joint_ie: ExtractionJointSchema | Unset
        if isinstance(_joint_ie, Unset):
            joint_ie = UNSET
        else:
            joint_ie = ExtractionJointSchema.from_dict(_joint_ie)

        extraction_schema = cls(
            entities=entities,
            relations=relations,
            classifications=classifications,
            structures=structures,
            entity_definitions=entity_definitions,
            entity_attributes=entity_attributes,
            classification_constraints=classification_constraints,
            joint_ie=joint_ie,
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
