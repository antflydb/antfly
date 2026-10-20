from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_operation_phase import MemoryOperationPhase

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

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
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
        )

        return memory_operation
