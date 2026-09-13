from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_joint_constraint_acyclic_relation import ExtractionJointConstraintAcyclicRelation
    from ..models.extraction_joint_constraint_entity_overlap_policy import ExtractionJointConstraintEntityOverlapPolicy
    from ..models.extraction_joint_constraint_inverse_relation import ExtractionJointConstraintInverseRelation
    from ..models.extraction_joint_constraint_max_relations_per_head import ExtractionJointConstraintMaxRelationsPerHead
    from ..models.extraction_joint_constraint_max_relations_per_tail import ExtractionJointConstraintMaxRelationsPerTail
    from ..models.extraction_joint_constraint_no_self_loops import ExtractionJointConstraintNoSelfLoops
    from ..models.extraction_joint_constraint_symmetric_relation import ExtractionJointConstraintSymmetricRelation
    from ..models.extraction_joint_constraint_typed_endpoints import ExtractionJointConstraintTypedEndpoints
    from ..models.extraction_joint_constraint_unique_relation_pair import ExtractionJointConstraintUniqueRelationPair
    from ..models.extraction_joint_constraint_unique_relation_slot import ExtractionJointConstraintUniqueRelationSlot
    from ..models.extraction_joint_schema_entities import ExtractionJointSchemaEntities
    from ..models.extraction_joint_schema_relations import ExtractionJointSchemaRelations


T = TypeVar("T", bound="ExtractionJointSchema")


@_attrs_define
class ExtractionJointSchema:
    """Separate typed graph schema, mutually exclusive with ordinary extraction families. Hard typed endpoints, overlap,
    uniqueness and declared graph constraints apply to every returned edge, including derived companions.

        Attributes:
            entities (ExtractionJointSchemaEntities):
            relations (ExtractionJointSchemaRelations | Unset):
            constraints (list[ExtractionJointConstraintAcyclicRelation | ExtractionJointConstraintEntityOverlapPolicy |
                ExtractionJointConstraintInverseRelation | ExtractionJointConstraintMaxRelationsPerHead |
                ExtractionJointConstraintMaxRelationsPerTail | ExtractionJointConstraintNoSelfLoops |
                ExtractionJointConstraintSymmetricRelation | ExtractionJointConstraintTypedEndpoints |
                ExtractionJointConstraintUniqueRelationPair | ExtractionJointConstraintUniqueRelationSlot] | Unset):
    """

    entities: ExtractionJointSchemaEntities
    relations: ExtractionJointSchemaRelations | Unset = UNSET
    constraints: (
        list[
            ExtractionJointConstraintAcyclicRelation
            | ExtractionJointConstraintEntityOverlapPolicy
            | ExtractionJointConstraintInverseRelation
            | ExtractionJointConstraintMaxRelationsPerHead
            | ExtractionJointConstraintMaxRelationsPerTail
            | ExtractionJointConstraintNoSelfLoops
            | ExtractionJointConstraintSymmetricRelation
            | ExtractionJointConstraintTypedEndpoints
            | ExtractionJointConstraintUniqueRelationPair
            | ExtractionJointConstraintUniqueRelationSlot
        ]
        | Unset
    ) = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.extraction_joint_constraint_acyclic_relation import ExtractionJointConstraintAcyclicRelation
        from ..models.extraction_joint_constraint_entity_overlap_policy import (
            ExtractionJointConstraintEntityOverlapPolicy,
        )
        from ..models.extraction_joint_constraint_max_relations_per_head import (
            ExtractionJointConstraintMaxRelationsPerHead,
        )
        from ..models.extraction_joint_constraint_max_relations_per_tail import (
            ExtractionJointConstraintMaxRelationsPerTail,
        )
        from ..models.extraction_joint_constraint_no_self_loops import ExtractionJointConstraintNoSelfLoops
        from ..models.extraction_joint_constraint_symmetric_relation import ExtractionJointConstraintSymmetricRelation
        from ..models.extraction_joint_constraint_typed_endpoints import ExtractionJointConstraintTypedEndpoints
        from ..models.extraction_joint_constraint_unique_relation_pair import (
            ExtractionJointConstraintUniqueRelationPair,
        )
        from ..models.extraction_joint_constraint_unique_relation_slot import (
            ExtractionJointConstraintUniqueRelationSlot,
        )

        entities = self.entities.to_dict()

        relations: dict[str, Any] | Unset = UNSET
        if not isinstance(self.relations, Unset):
            relations = self.relations.to_dict()

        constraints: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.constraints, Unset):
            constraints = []
            for constraints_item_data in self.constraints:
                constraints_item: dict[str, Any]
                if isinstance(constraints_item_data, ExtractionJointConstraintTypedEndpoints):
                    constraints_item = constraints_item_data.to_dict()
                elif isinstance(constraints_item_data, ExtractionJointConstraintNoSelfLoops):
                    constraints_item = constraints_item_data.to_dict()
                elif isinstance(constraints_item_data, ExtractionJointConstraintUniqueRelationPair):
                    constraints_item = constraints_item_data.to_dict()
                elif isinstance(constraints_item_data, ExtractionJointConstraintUniqueRelationSlot):
                    constraints_item = constraints_item_data.to_dict()
                elif isinstance(constraints_item_data, ExtractionJointConstraintEntityOverlapPolicy):
                    constraints_item = constraints_item_data.to_dict()
                elif isinstance(constraints_item_data, ExtractionJointConstraintMaxRelationsPerHead):
                    constraints_item = constraints_item_data.to_dict()
                elif isinstance(constraints_item_data, ExtractionJointConstraintMaxRelationsPerTail):
                    constraints_item = constraints_item_data.to_dict()
                elif isinstance(constraints_item_data, ExtractionJointConstraintSymmetricRelation):
                    constraints_item = constraints_item_data.to_dict()
                elif isinstance(constraints_item_data, ExtractionJointConstraintAcyclicRelation):
                    constraints_item = constraints_item_data.to_dict()
                else:
                    constraints_item = constraints_item_data.to_dict()

                constraints.append(constraints_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "entities": entities,
            }
        )
        if relations is not UNSET:
            field_dict["relations"] = relations
        if constraints is not UNSET:
            field_dict["constraints"] = constraints

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_joint_constraint_acyclic_relation import ExtractionJointConstraintAcyclicRelation
        from ..models.extraction_joint_constraint_entity_overlap_policy import (
            ExtractionJointConstraintEntityOverlapPolicy,
        )
        from ..models.extraction_joint_constraint_inverse_relation import ExtractionJointConstraintInverseRelation
        from ..models.extraction_joint_constraint_max_relations_per_head import (
            ExtractionJointConstraintMaxRelationsPerHead,
        )
        from ..models.extraction_joint_constraint_max_relations_per_tail import (
            ExtractionJointConstraintMaxRelationsPerTail,
        )
        from ..models.extraction_joint_constraint_no_self_loops import ExtractionJointConstraintNoSelfLoops
        from ..models.extraction_joint_constraint_symmetric_relation import ExtractionJointConstraintSymmetricRelation
        from ..models.extraction_joint_constraint_typed_endpoints import ExtractionJointConstraintTypedEndpoints
        from ..models.extraction_joint_constraint_unique_relation_pair import (
            ExtractionJointConstraintUniqueRelationPair,
        )
        from ..models.extraction_joint_constraint_unique_relation_slot import (
            ExtractionJointConstraintUniqueRelationSlot,
        )
        from ..models.extraction_joint_schema_entities import ExtractionJointSchemaEntities
        from ..models.extraction_joint_schema_relations import ExtractionJointSchemaRelations

        d = dict(src_dict)
        entities = ExtractionJointSchemaEntities.from_dict(d.pop("entities"))

        _relations = d.pop("relations", UNSET)
        relations: ExtractionJointSchemaRelations | Unset
        if isinstance(_relations, Unset):
            relations = UNSET
        else:
            relations = ExtractionJointSchemaRelations.from_dict(_relations)

        _constraints = d.pop("constraints", UNSET)
        constraints: (
            list[
                ExtractionJointConstraintAcyclicRelation
                | ExtractionJointConstraintEntityOverlapPolicy
                | ExtractionJointConstraintInverseRelation
                | ExtractionJointConstraintMaxRelationsPerHead
                | ExtractionJointConstraintMaxRelationsPerTail
                | ExtractionJointConstraintNoSelfLoops
                | ExtractionJointConstraintSymmetricRelation
                | ExtractionJointConstraintTypedEndpoints
                | ExtractionJointConstraintUniqueRelationPair
                | ExtractionJointConstraintUniqueRelationSlot
            ]
            | Unset
        ) = UNSET
        if _constraints is not UNSET:
            constraints = []
            for constraints_item_data in _constraints:

                def _parse_constraints_item(
                    data: object,
                ) -> (
                    ExtractionJointConstraintAcyclicRelation
                    | ExtractionJointConstraintEntityOverlapPolicy
                    | ExtractionJointConstraintInverseRelation
                    | ExtractionJointConstraintMaxRelationsPerHead
                    | ExtractionJointConstraintMaxRelationsPerTail
                    | ExtractionJointConstraintNoSelfLoops
                    | ExtractionJointConstraintSymmetricRelation
                    | ExtractionJointConstraintTypedEndpoints
                    | ExtractionJointConstraintUniqueRelationPair
                    | ExtractionJointConstraintUniqueRelationSlot
                ):
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_joint_constraint_type_0 = (
                            ExtractionJointConstraintTypedEndpoints.from_dict(data)
                        )

                        return componentsschemas_extraction_joint_constraint_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_joint_constraint_type_1 = (
                            ExtractionJointConstraintNoSelfLoops.from_dict(data)
                        )

                        return componentsschemas_extraction_joint_constraint_type_1
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_joint_constraint_type_2 = (
                            ExtractionJointConstraintUniqueRelationPair.from_dict(data)
                        )

                        return componentsschemas_extraction_joint_constraint_type_2
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_joint_constraint_type_3 = (
                            ExtractionJointConstraintUniqueRelationSlot.from_dict(data)
                        )

                        return componentsschemas_extraction_joint_constraint_type_3
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_joint_constraint_type_4 = (
                            ExtractionJointConstraintEntityOverlapPolicy.from_dict(data)
                        )

                        return componentsschemas_extraction_joint_constraint_type_4
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_joint_constraint_type_5 = (
                            ExtractionJointConstraintMaxRelationsPerHead.from_dict(data)
                        )

                        return componentsschemas_extraction_joint_constraint_type_5
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_joint_constraint_type_6 = (
                            ExtractionJointConstraintMaxRelationsPerTail.from_dict(data)
                        )

                        return componentsschemas_extraction_joint_constraint_type_6
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_joint_constraint_type_7 = (
                            ExtractionJointConstraintSymmetricRelation.from_dict(data)
                        )

                        return componentsschemas_extraction_joint_constraint_type_7
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_joint_constraint_type_8 = (
                            ExtractionJointConstraintAcyclicRelation.from_dict(data)
                        )

                        return componentsschemas_extraction_joint_constraint_type_8
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_joint_constraint_type_9 = (
                        ExtractionJointConstraintInverseRelation.from_dict(data)
                    )

                    return componentsschemas_extraction_joint_constraint_type_9

                constraints_item = _parse_constraints_item(constraints_item_data)

                constraints.append(constraints_item)

        extraction_joint_schema = cls(
            entities=entities,
            relations=relations,
            constraints=constraints,
        )

        return extraction_joint_schema
