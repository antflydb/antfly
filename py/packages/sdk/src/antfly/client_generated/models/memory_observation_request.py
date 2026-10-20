from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_assertion import MemoryAssertion
    from ..models.memory_evidence import MemoryEvidence
    from ..models.memory_observation_request_entities_item import MemoryObservationRequestEntitiesItem


T = TypeVar("T", bound="MemoryObservationRequest")


@_attrs_define
class MemoryObservationRequest:
    """
    Attributes:
        evidence (list[MemoryEvidence]):
        context_evidence_ids (list[str] | Unset):
        assertions (list[MemoryAssertion] | Unset):
        entities (list[MemoryObservationRequestEntitiesItem] | Unset):
    """

    evidence: list[MemoryEvidence]
    context_evidence_ids: list[str] | Unset = UNSET
    assertions: list[MemoryAssertion] | Unset = UNSET
    entities: list[MemoryObservationRequestEntitiesItem] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        evidence = []
        for evidence_item_data in self.evidence:
            evidence_item = evidence_item_data.to_dict()
            evidence.append(evidence_item)

        context_evidence_ids: list[str] | Unset = UNSET
        if not isinstance(self.context_evidence_ids, Unset):
            context_evidence_ids = self.context_evidence_ids

        assertions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.assertions, Unset):
            assertions = []
            for assertions_item_data in self.assertions:
                assertions_item = assertions_item_data.to_dict()
                assertions.append(assertions_item)

        entities: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.entities, Unset):
            entities = []
            for entities_item_data in self.entities:
                entities_item = entities_item_data.to_dict()
                entities.append(entities_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "evidence": evidence,
            }
        )
        if context_evidence_ids is not UNSET:
            field_dict["context_evidence_ids"] = context_evidence_ids
        if assertions is not UNSET:
            field_dict["assertions"] = assertions
        if entities is not UNSET:
            field_dict["entities"] = entities

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_assertion import MemoryAssertion
        from ..models.memory_evidence import MemoryEvidence
        from ..models.memory_observation_request_entities_item import MemoryObservationRequestEntitiesItem

        d = dict(src_dict)
        evidence = []
        _evidence = d.pop("evidence")
        for evidence_item_data in _evidence:
            evidence_item = MemoryEvidence.from_dict(evidence_item_data)

            evidence.append(evidence_item)

        context_evidence_ids = cast(list[str], d.pop("context_evidence_ids", UNSET))

        _assertions = d.pop("assertions", UNSET)
        assertions: list[MemoryAssertion] | Unset = UNSET
        if _assertions is not UNSET:
            assertions = []
            for assertions_item_data in _assertions:
                assertions_item = MemoryAssertion.from_dict(assertions_item_data)

                assertions.append(assertions_item)

        _entities = d.pop("entities", UNSET)
        entities: list[MemoryObservationRequestEntitiesItem] | Unset = UNSET
        if _entities is not UNSET:
            entities = []
            for entities_item_data in _entities:
                entities_item = MemoryObservationRequestEntitiesItem.from_dict(entities_item_data)

                entities.append(entities_item)

        memory_observation_request = cls(
            evidence=evidence,
            context_evidence_ids=context_evidence_ids,
            assertions=assertions,
            entities=entities,
        )

        return memory_observation_request
