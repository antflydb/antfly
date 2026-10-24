from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_entity_proposal_action import MemoryEntityProposalAction
from ..models.memory_entity_proposal_state import MemoryEntityProposalState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity_proposal_decided_type_0 import MemoryEntityProposalDecidedType0
    from ..models.memory_entity_proposal_options import MemoryEntityProposalOptions
    from ..models.memory_entity_proposal_origin import MemoryEntityProposalOrigin


T = TypeVar("T", bound="MemoryEntityProposal")


@_attrs_define
class MemoryEntityProposal:
    """
    Attributes:
        id (str):
        action (MemoryEntityProposalAction):
        key (str):
        origin (MemoryEntityProposalOrigin):
        into (None | str | Unset):
        options (MemoryEntityProposalOptions | Unset):
        confidence (float | None | Unset):
        evidence (Any | Unset):
        state (MemoryEntityProposalState | Unset):  Default: MemoryEntityProposalState.OPEN.
        decided (MemoryEntityProposalDecidedType0 | None | Unset):
    """

    id: str
    action: MemoryEntityProposalAction
    key: str
    origin: MemoryEntityProposalOrigin
    into: None | str | Unset = UNSET
    options: MemoryEntityProposalOptions | Unset = UNSET
    confidence: float | None | Unset = UNSET
    evidence: Any | Unset = UNSET
    state: MemoryEntityProposalState | Unset = MemoryEntityProposalState.OPEN
    decided: MemoryEntityProposalDecidedType0 | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_entity_proposal_decided_type_0 import MemoryEntityProposalDecidedType0

        id = self.id

        action = self.action.value

        key = self.key

        origin = self.origin.to_dict()

        into: None | str | Unset
        if isinstance(self.into, Unset):
            into = UNSET
        else:
            into = self.into

        options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.options, Unset):
            options = self.options.to_dict()

        confidence: float | None | Unset
        if isinstance(self.confidence, Unset):
            confidence = UNSET
        else:
            confidence = self.confidence

        evidence = self.evidence

        state: str | Unset = UNSET
        if not isinstance(self.state, Unset):
            state = self.state.value

        decided: dict[str, Any] | None | Unset
        if isinstance(self.decided, Unset):
            decided = UNSET
        elif isinstance(self.decided, MemoryEntityProposalDecidedType0):
            decided = self.decided.to_dict()
        else:
            decided = self.decided

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "action": action,
                "key": key,
                "origin": origin,
            }
        )
        if into is not UNSET:
            field_dict["into"] = into
        if options is not UNSET:
            field_dict["options"] = options
        if confidence is not UNSET:
            field_dict["confidence"] = confidence
        if evidence is not UNSET:
            field_dict["evidence"] = evidence
        if state is not UNSET:
            field_dict["state"] = state
        if decided is not UNSET:
            field_dict["decided"] = decided

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_proposal_decided_type_0 import MemoryEntityProposalDecidedType0
        from ..models.memory_entity_proposal_options import MemoryEntityProposalOptions
        from ..models.memory_entity_proposal_origin import MemoryEntityProposalOrigin

        d = dict(src_dict)
        id = d.pop("id")

        action = MemoryEntityProposalAction(d.pop("action"))

        key = d.pop("key")

        origin = MemoryEntityProposalOrigin.from_dict(d.pop("origin"))

        def _parse_into(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        into = _parse_into(d.pop("into", UNSET))

        _options = d.pop("options", UNSET)
        options: MemoryEntityProposalOptions | Unset
        if isinstance(_options, Unset):
            options = UNSET
        else:
            options = MemoryEntityProposalOptions.from_dict(_options)

        def _parse_confidence(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        confidence = _parse_confidence(d.pop("confidence", UNSET))

        evidence = d.pop("evidence", UNSET)

        _state = d.pop("state", UNSET)
        state: MemoryEntityProposalState | Unset
        if isinstance(_state, Unset):
            state = UNSET
        else:
            state = MemoryEntityProposalState(_state)

        def _parse_decided(data: object) -> MemoryEntityProposalDecidedType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                decided_type_0 = MemoryEntityProposalDecidedType0.from_dict(data)

                return decided_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEntityProposalDecidedType0 | None | Unset, data)

        decided = _parse_decided(d.pop("decided", UNSET))

        memory_entity_proposal = cls(
            id=id,
            action=action,
            key=key,
            origin=origin,
            into=into,
            options=options,
            confidence=confidence,
            evidence=evidence,
            state=state,
            decided=decided,
        )

        return memory_entity_proposal
