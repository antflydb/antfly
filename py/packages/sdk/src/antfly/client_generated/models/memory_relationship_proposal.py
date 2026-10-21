from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_relationship_proposal_state import MemoryRelationshipProposalState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_relationship_proposal_assertion import MemoryRelationshipProposalAssertion
    from ..models.memory_relationship_proposal_model_type_0 import MemoryRelationshipProposalModelType0
    from ..models.memory_relationship_proposal_verdict_evidence_item import (
        MemoryRelationshipProposalVerdictEvidenceItem,
    )


T = TypeVar("T", bound="MemoryRelationshipProposal")


@_attrs_define
class MemoryRelationshipProposal:
    """
    Attributes:
        id (str):
        assertion (MemoryRelationshipProposalAssertion):
        owner (str):
        actor (str):
        config_generation (int):
        revision (int):
        recorded_at (int):
        state (MemoryRelationshipProposalState | Unset):  Default: MemoryRelationshipProposalState.OPEN.
        verdict_actor (str | Unset):
        verdict_principal (str | Unset):
        verdict_revision (int | None | Unset):
        model (MemoryRelationshipProposalModelType0 | None | Unset):
        verdict_evidence (list[MemoryRelationshipProposalVerdictEvidenceItem] | Unset):
        reason (str | Unset):
    """

    id: str
    assertion: MemoryRelationshipProposalAssertion
    owner: str
    actor: str
    config_generation: int
    revision: int
    recorded_at: int
    state: MemoryRelationshipProposalState | Unset = MemoryRelationshipProposalState.OPEN
    verdict_actor: str | Unset = UNSET
    verdict_principal: str | Unset = UNSET
    verdict_revision: int | None | Unset = UNSET
    model: MemoryRelationshipProposalModelType0 | None | Unset = UNSET
    verdict_evidence: list[MemoryRelationshipProposalVerdictEvidenceItem] | Unset = UNSET
    reason: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_relationship_proposal_model_type_0 import MemoryRelationshipProposalModelType0

        id = self.id

        assertion = self.assertion.to_dict()

        owner = self.owner

        actor = self.actor

        config_generation = self.config_generation

        revision = self.revision

        recorded_at = self.recorded_at

        state: str | Unset = UNSET
        if not isinstance(self.state, Unset):
            state = self.state.value

        verdict_actor = self.verdict_actor

        verdict_principal = self.verdict_principal

        verdict_revision: int | None | Unset
        if isinstance(self.verdict_revision, Unset):
            verdict_revision = UNSET
        else:
            verdict_revision = self.verdict_revision

        model: dict[str, Any] | None | Unset
        if isinstance(self.model, Unset):
            model = UNSET
        elif isinstance(self.model, MemoryRelationshipProposalModelType0):
            model = self.model.to_dict()
        else:
            model = self.model

        verdict_evidence: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.verdict_evidence, Unset):
            verdict_evidence = []
            for verdict_evidence_item_data in self.verdict_evidence:
                verdict_evidence_item = verdict_evidence_item_data.to_dict()
                verdict_evidence.append(verdict_evidence_item)

        reason = self.reason

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "assertion": assertion,
                "owner": owner,
                "actor": actor,
                "config_generation": config_generation,
                "revision": revision,
                "recorded_at": recorded_at,
            }
        )
        if state is not UNSET:
            field_dict["state"] = state
        if verdict_actor is not UNSET:
            field_dict["verdict_actor"] = verdict_actor
        if verdict_principal is not UNSET:
            field_dict["verdict_principal"] = verdict_principal
        if verdict_revision is not UNSET:
            field_dict["verdict_revision"] = verdict_revision
        if model is not UNSET:
            field_dict["model"] = model
        if verdict_evidence is not UNSET:
            field_dict["verdict_evidence"] = verdict_evidence
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_relationship_proposal_assertion import MemoryRelationshipProposalAssertion
        from ..models.memory_relationship_proposal_model_type_0 import MemoryRelationshipProposalModelType0
        from ..models.memory_relationship_proposal_verdict_evidence_item import (
            MemoryRelationshipProposalVerdictEvidenceItem,
        )

        d = dict(src_dict)
        id = d.pop("id")

        assertion = MemoryRelationshipProposalAssertion.from_dict(d.pop("assertion"))

        owner = d.pop("owner")

        actor = d.pop("actor")

        config_generation = d.pop("config_generation")

        revision = d.pop("revision")

        recorded_at = d.pop("recorded_at")

        _state = d.pop("state", UNSET)
        state: MemoryRelationshipProposalState | Unset
        if isinstance(_state, Unset):
            state = UNSET
        else:
            state = MemoryRelationshipProposalState(_state)

        verdict_actor = d.pop("verdict_actor", UNSET)

        verdict_principal = d.pop("verdict_principal", UNSET)

        def _parse_verdict_revision(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        verdict_revision = _parse_verdict_revision(d.pop("verdict_revision", UNSET))

        def _parse_model(data: object) -> MemoryRelationshipProposalModelType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                model_type_0 = MemoryRelationshipProposalModelType0.from_dict(data)

                return model_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryRelationshipProposalModelType0 | None | Unset, data)

        model = _parse_model(d.pop("model", UNSET))

        _verdict_evidence = d.pop("verdict_evidence", UNSET)
        verdict_evidence: list[MemoryRelationshipProposalVerdictEvidenceItem] | Unset = UNSET
        if _verdict_evidence is not UNSET:
            verdict_evidence = []
            for verdict_evidence_item_data in _verdict_evidence:
                verdict_evidence_item = MemoryRelationshipProposalVerdictEvidenceItem.from_dict(
                    verdict_evidence_item_data
                )

                verdict_evidence.append(verdict_evidence_item)

        reason = d.pop("reason", UNSET)

        memory_relationship_proposal = cls(
            id=id,
            assertion=assertion,
            owner=owner,
            actor=actor,
            config_generation=config_generation,
            revision=revision,
            recorded_at=recorded_at,
            state=state,
            verdict_actor=verdict_actor,
            verdict_principal=verdict_principal,
            verdict_revision=verdict_revision,
            model=model,
            verdict_evidence=verdict_evidence,
            reason=reason,
        )

        return memory_relationship_proposal
