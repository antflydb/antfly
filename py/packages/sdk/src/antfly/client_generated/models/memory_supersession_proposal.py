from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_supersession_proposal_verdict import MemorySupersessionProposalVerdict
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_claim_pin import MemoryClaimPin
    from ..models.memory_supersession_proposal_evidence_item import MemorySupersessionProposalEvidenceItem
    from ..models.memory_supersession_proposal_model_type_0 import MemorySupersessionProposalModelType0


T = TypeVar("T", bound="MemorySupersessionProposal")


@_attrs_define
class MemorySupersessionProposal:
    """
    Attributes:
        id (str):
        incoming (MemoryClaimPin):
        previous (MemoryClaimPin):
        owner (str):
        actor (str):
        config_generation (int):
        revision (int):
        recorded_at (int):
        branch (str | Unset):
        verdict (MemorySupersessionProposalVerdict | Unset):
        verdict_revision (int | None | Unset):
        verdict_principal (str | Unset):
        verdict_actor (str | Unset):
        reason (str | Unset):
        evidence (list[MemorySupersessionProposalEvidenceItem] | Unset):
        model (MemorySupersessionProposalModelType0 | None | Unset):
    """

    id: str
    incoming: MemoryClaimPin
    previous: MemoryClaimPin
    owner: str
    actor: str
    config_generation: int
    revision: int
    recorded_at: int
    branch: str | Unset = UNSET
    verdict: MemorySupersessionProposalVerdict | Unset = UNSET
    verdict_revision: int | None | Unset = UNSET
    verdict_principal: str | Unset = UNSET
    verdict_actor: str | Unset = UNSET
    reason: str | Unset = UNSET
    evidence: list[MemorySupersessionProposalEvidenceItem] | Unset = UNSET
    model: MemorySupersessionProposalModelType0 | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_supersession_proposal_model_type_0 import MemorySupersessionProposalModelType0

        id = self.id

        incoming = self.incoming.to_dict()

        previous = self.previous.to_dict()

        owner = self.owner

        actor = self.actor

        config_generation = self.config_generation

        revision = self.revision

        recorded_at = self.recorded_at

        branch = self.branch

        verdict: str | Unset = UNSET
        if not isinstance(self.verdict, Unset):
            verdict = self.verdict.value

        verdict_revision: int | None | Unset
        if isinstance(self.verdict_revision, Unset):
            verdict_revision = UNSET
        else:
            verdict_revision = self.verdict_revision

        verdict_principal = self.verdict_principal

        verdict_actor = self.verdict_actor

        reason = self.reason

        evidence: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.evidence, Unset):
            evidence = []
            for evidence_item_data in self.evidence:
                evidence_item = evidence_item_data.to_dict()
                evidence.append(evidence_item)

        model: dict[str, Any] | None | Unset
        if isinstance(self.model, Unset):
            model = UNSET
        elif isinstance(self.model, MemorySupersessionProposalModelType0):
            model = self.model.to_dict()
        else:
            model = self.model

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "incoming": incoming,
                "previous": previous,
                "owner": owner,
                "actor": actor,
                "config_generation": config_generation,
                "revision": revision,
                "recorded_at": recorded_at,
            }
        )
        if branch is not UNSET:
            field_dict["branch"] = branch
        if verdict is not UNSET:
            field_dict["verdict"] = verdict
        if verdict_revision is not UNSET:
            field_dict["verdict_revision"] = verdict_revision
        if verdict_principal is not UNSET:
            field_dict["verdict_principal"] = verdict_principal
        if verdict_actor is not UNSET:
            field_dict["verdict_actor"] = verdict_actor
        if reason is not UNSET:
            field_dict["reason"] = reason
        if evidence is not UNSET:
            field_dict["evidence"] = evidence
        if model is not UNSET:
            field_dict["model"] = model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_claim_pin import MemoryClaimPin
        from ..models.memory_supersession_proposal_evidence_item import MemorySupersessionProposalEvidenceItem
        from ..models.memory_supersession_proposal_model_type_0 import MemorySupersessionProposalModelType0

        d = dict(src_dict)
        id = d.pop("id")

        incoming = MemoryClaimPin.from_dict(d.pop("incoming"))

        previous = MemoryClaimPin.from_dict(d.pop("previous"))

        owner = d.pop("owner")

        actor = d.pop("actor")

        config_generation = d.pop("config_generation")

        revision = d.pop("revision")

        recorded_at = d.pop("recorded_at")

        branch = d.pop("branch", UNSET)

        _verdict = d.pop("verdict", UNSET)
        verdict: MemorySupersessionProposalVerdict | Unset
        if isinstance(_verdict, Unset):
            verdict = UNSET
        else:
            verdict = MemorySupersessionProposalVerdict(_verdict)

        def _parse_verdict_revision(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        verdict_revision = _parse_verdict_revision(d.pop("verdict_revision", UNSET))

        verdict_principal = d.pop("verdict_principal", UNSET)

        verdict_actor = d.pop("verdict_actor", UNSET)

        reason = d.pop("reason", UNSET)

        _evidence = d.pop("evidence", UNSET)
        evidence: list[MemorySupersessionProposalEvidenceItem] | Unset = UNSET
        if _evidence is not UNSET:
            evidence = []
            for evidence_item_data in _evidence:
                evidence_item = MemorySupersessionProposalEvidenceItem.from_dict(evidence_item_data)

                evidence.append(evidence_item)

        def _parse_model(data: object) -> MemorySupersessionProposalModelType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                model_type_0 = MemorySupersessionProposalModelType0.from_dict(data)

                return model_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemorySupersessionProposalModelType0 | None | Unset, data)

        model = _parse_model(d.pop("model", UNSET))

        memory_supersession_proposal = cls(
            id=id,
            incoming=incoming,
            previous=previous,
            owner=owner,
            actor=actor,
            config_generation=config_generation,
            revision=revision,
            recorded_at=recorded_at,
            branch=branch,
            verdict=verdict,
            verdict_revision=verdict_revision,
            verdict_principal=verdict_principal,
            verdict_actor=verdict_actor,
            reason=reason,
            evidence=evidence,
            model=model,
        )

        return memory_supersession_proposal
