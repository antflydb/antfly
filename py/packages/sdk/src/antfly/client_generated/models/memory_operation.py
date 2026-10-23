from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_operation_phase import MemoryOperationPhase
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_intake_screening_report import MemoryIntakeScreeningReport


T = TypeVar("T", bound="MemoryOperation")


@_attrs_define
class MemoryOperation:
    """
    Attributes:
        id (str):
        action (str):
        request_hash (str):
        owner (str):
        revision (int):
        config_generation (int):
        created_at (int):
        phase (MemoryOperationPhase):
        evidence_accepted (bool):
        assertions_ready (bool):
        attempts (int):
        error_code (None | str):
        next_retry_at (int | None):
        evaluation_id (None | str | Unset):
        branch (str | Unset):
        intake_screening (MemoryIntakeScreeningReport | Unset):
        proposal_id (None | str | Unset):
    """

    id: str
    action: str
    request_hash: str
    owner: str
    revision: int
    config_generation: int
    created_at: int
    phase: MemoryOperationPhase
    evidence_accepted: bool
    assertions_ready: bool
    attempts: int
    error_code: None | str
    next_retry_at: int | None
    evaluation_id: None | str | Unset = UNSET
    branch: str | Unset = UNSET
    intake_screening: MemoryIntakeScreeningReport | Unset = UNSET
    proposal_id: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        action = self.action

        request_hash = self.request_hash

        owner = self.owner

        revision = self.revision

        config_generation = self.config_generation

        created_at = self.created_at

        phase = self.phase.value

        evidence_accepted = self.evidence_accepted

        assertions_ready = self.assertions_ready

        attempts = self.attempts

        error_code: None | str
        error_code = self.error_code

        next_retry_at: int | None
        next_retry_at = self.next_retry_at

        evaluation_id: None | str | Unset
        if isinstance(self.evaluation_id, Unset):
            evaluation_id = UNSET
        else:
            evaluation_id = self.evaluation_id

        branch = self.branch

        intake_screening: dict[str, Any] | Unset = UNSET
        if not isinstance(self.intake_screening, Unset):
            intake_screening = self.intake_screening.to_dict()

        proposal_id: None | str | Unset
        if isinstance(self.proposal_id, Unset):
            proposal_id = UNSET
        else:
            proposal_id = self.proposal_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "action": action,
                "request_hash": request_hash,
                "owner": owner,
                "revision": revision,
                "config_generation": config_generation,
                "created_at": created_at,
                "phase": phase,
                "evidence_accepted": evidence_accepted,
                "assertions_ready": assertions_ready,
                "attempts": attempts,
                "error_code": error_code,
                "next_retry_at": next_retry_at,
            }
        )
        if evaluation_id is not UNSET:
            field_dict["evaluation_id"] = evaluation_id
        if branch is not UNSET:
            field_dict["branch"] = branch
        if intake_screening is not UNSET:
            field_dict["intake_screening"] = intake_screening
        if proposal_id is not UNSET:
            field_dict["proposal_id"] = proposal_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_intake_screening_report import MemoryIntakeScreeningReport

        d = dict(src_dict)
        id = d.pop("id")

        action = d.pop("action")

        request_hash = d.pop("request_hash")

        owner = d.pop("owner")

        revision = d.pop("revision")

        config_generation = d.pop("config_generation")

        created_at = d.pop("created_at")

        phase = MemoryOperationPhase(d.pop("phase"))

        evidence_accepted = d.pop("evidence_accepted")

        assertions_ready = d.pop("assertions_ready")

        attempts = d.pop("attempts")

        def _parse_error_code(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        error_code = _parse_error_code(d.pop("error_code"))

        def _parse_next_retry_at(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        next_retry_at = _parse_next_retry_at(d.pop("next_retry_at"))

        def _parse_evaluation_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        evaluation_id = _parse_evaluation_id(d.pop("evaluation_id", UNSET))

        branch = d.pop("branch", UNSET)

        _intake_screening = d.pop("intake_screening", UNSET)
        intake_screening: MemoryIntakeScreeningReport | Unset
        if isinstance(_intake_screening, Unset):
            intake_screening = UNSET
        else:
            intake_screening = MemoryIntakeScreeningReport.from_dict(_intake_screening)

        def _parse_proposal_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        proposal_id = _parse_proposal_id(d.pop("proposal_id", UNSET))

        memory_operation = cls(
            id=id,
            action=action,
            request_hash=request_hash,
            owner=owner,
            revision=revision,
            config_generation=config_generation,
            created_at=created_at,
            phase=phase,
            evidence_accepted=evidence_accepted,
            assertions_ready=assertions_ready,
            attempts=attempts,
            error_code=error_code,
            next_retry_at=next_retry_at,
            evaluation_id=evaluation_id,
            branch=branch,
            intake_screening=intake_screening,
            proposal_id=proposal_id,
        )

        return memory_operation
