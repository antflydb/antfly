from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity_proposal_inputs_entities_item import MemoryEntityProposalInputsEntitiesItem
    from ..models.memory_entity_proposal_inputs_evidence_item import MemoryEntityProposalInputsEvidenceItem
    from ..models.memory_entity_proposal_inputs_model_type_0 import MemoryEntityProposalInputsModelType0
    from ..models.memory_entity_proposal_inputs_operation_type_0 import MemoryEntityProposalInputsOperationType0


T = TypeVar("T", bound="MemoryEntityProposalInputs")


@_attrs_define
class MemoryEntityProposalInputs:
    """
    Attributes:
        owner (str):
        config_generation (int):
        entities (list[MemoryEntityProposalInputsEntitiesItem]):
        evidence (list[MemoryEntityProposalInputsEvidenceItem]):
        revision (int | Unset):  Default: 0.
        operation (MemoryEntityProposalInputsOperationType0 | None | Unset):
        actor (str | Unset):
        verdict_actor (str | Unset):
        verdict_revision (int | None | Unset):
        model (MemoryEntityProposalInputsModelType0 | None | Unset):
    """

    owner: str
    config_generation: int
    entities: list[MemoryEntityProposalInputsEntitiesItem]
    evidence: list[MemoryEntityProposalInputsEvidenceItem]
    revision: int | Unset = 0
    operation: MemoryEntityProposalInputsOperationType0 | None | Unset = UNSET
    actor: str | Unset = UNSET
    verdict_actor: str | Unset = UNSET
    verdict_revision: int | None | Unset = UNSET
    model: MemoryEntityProposalInputsModelType0 | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_entity_proposal_inputs_model_type_0 import MemoryEntityProposalInputsModelType0
        from ..models.memory_entity_proposal_inputs_operation_type_0 import MemoryEntityProposalInputsOperationType0

        owner = self.owner

        config_generation = self.config_generation

        entities = []
        for entities_item_data in self.entities:
            entities_item = entities_item_data.to_dict()
            entities.append(entities_item)

        evidence = []
        for evidence_item_data in self.evidence:
            evidence_item = evidence_item_data.to_dict()
            evidence.append(evidence_item)

        revision = self.revision

        operation: dict[str, Any] | None | Unset
        if isinstance(self.operation, Unset):
            operation = UNSET
        elif isinstance(self.operation, MemoryEntityProposalInputsOperationType0):
            operation = self.operation.to_dict()
        else:
            operation = self.operation

        actor = self.actor

        verdict_actor = self.verdict_actor

        verdict_revision: int | None | Unset
        if isinstance(self.verdict_revision, Unset):
            verdict_revision = UNSET
        else:
            verdict_revision = self.verdict_revision

        model: dict[str, Any] | None | Unset
        if isinstance(self.model, Unset):
            model = UNSET
        elif isinstance(self.model, MemoryEntityProposalInputsModelType0):
            model = self.model.to_dict()
        else:
            model = self.model

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "owner": owner,
                "config_generation": config_generation,
                "entities": entities,
                "evidence": evidence,
            }
        )
        if revision is not UNSET:
            field_dict["revision"] = revision
        if operation is not UNSET:
            field_dict["operation"] = operation
        if actor is not UNSET:
            field_dict["actor"] = actor
        if verdict_actor is not UNSET:
            field_dict["verdict_actor"] = verdict_actor
        if verdict_revision is not UNSET:
            field_dict["verdict_revision"] = verdict_revision
        if model is not UNSET:
            field_dict["model"] = model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_proposal_inputs_entities_item import MemoryEntityProposalInputsEntitiesItem
        from ..models.memory_entity_proposal_inputs_evidence_item import MemoryEntityProposalInputsEvidenceItem
        from ..models.memory_entity_proposal_inputs_model_type_0 import MemoryEntityProposalInputsModelType0
        from ..models.memory_entity_proposal_inputs_operation_type_0 import MemoryEntityProposalInputsOperationType0

        d = dict(src_dict)
        owner = d.pop("owner")

        config_generation = d.pop("config_generation")

        entities = []
        _entities = d.pop("entities")
        for entities_item_data in _entities:
            entities_item = MemoryEntityProposalInputsEntitiesItem.from_dict(entities_item_data)

            entities.append(entities_item)

        evidence = []
        _evidence = d.pop("evidence")
        for evidence_item_data in _evidence:
            evidence_item = MemoryEntityProposalInputsEvidenceItem.from_dict(evidence_item_data)

            evidence.append(evidence_item)

        revision = d.pop("revision", UNSET)

        def _parse_operation(data: object) -> MemoryEntityProposalInputsOperationType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                operation_type_0 = MemoryEntityProposalInputsOperationType0.from_dict(data)

                return operation_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEntityProposalInputsOperationType0 | None | Unset, data)

        operation = _parse_operation(d.pop("operation", UNSET))

        actor = d.pop("actor", UNSET)

        verdict_actor = d.pop("verdict_actor", UNSET)

        def _parse_verdict_revision(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        verdict_revision = _parse_verdict_revision(d.pop("verdict_revision", UNSET))

        def _parse_model(data: object) -> MemoryEntityProposalInputsModelType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                model_type_0 = MemoryEntityProposalInputsModelType0.from_dict(data)

                return model_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEntityProposalInputsModelType0 | None | Unset, data)

        model = _parse_model(d.pop("model", UNSET))

        memory_entity_proposal_inputs = cls(
            owner=owner,
            config_generation=config_generation,
            entities=entities,
            evidence=evidence,
            revision=revision,
            operation=operation,
            actor=actor,
            verdict_actor=verdict_actor,
            verdict_revision=verdict_revision,
            model=model,
        )

        return memory_entity_proposal_inputs
