from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_entity_proposal_origin_origin import MemoryEntityProposalOriginOrigin
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEntityProposalOrigin")


@_attrs_define
class MemoryEntityProposalOrigin:
    """
    Attributes:
        origin (MemoryEntityProposalOriginOrigin | Unset):  Default: MemoryEntityProposalOriginOrigin.MANUAL.
        actor (str | Unset):
        reason (str | Unset):
        created_at_ms (int | Unset):  Default: 0.
        revision (int | Unset):  Default: 0.
        proposal_id (None | str | Unset):
        cascade_from (None | str | Unset):
    """

    origin: MemoryEntityProposalOriginOrigin | Unset = MemoryEntityProposalOriginOrigin.MANUAL
    actor: str | Unset = UNSET
    reason: str | Unset = UNSET
    created_at_ms: int | Unset = 0
    revision: int | Unset = 0
    proposal_id: None | str | Unset = UNSET
    cascade_from: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        origin: str | Unset = UNSET
        if not isinstance(self.origin, Unset):
            origin = self.origin.value

        actor = self.actor

        reason = self.reason

        created_at_ms = self.created_at_ms

        revision = self.revision

        proposal_id: None | str | Unset
        if isinstance(self.proposal_id, Unset):
            proposal_id = UNSET
        else:
            proposal_id = self.proposal_id

        cascade_from: None | str | Unset
        if isinstance(self.cascade_from, Unset):
            cascade_from = UNSET
        else:
            cascade_from = self.cascade_from

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if origin is not UNSET:
            field_dict["origin"] = origin
        if actor is not UNSET:
            field_dict["actor"] = actor
        if reason is not UNSET:
            field_dict["reason"] = reason
        if created_at_ms is not UNSET:
            field_dict["created_at_ms"] = created_at_ms
        if revision is not UNSET:
            field_dict["revision"] = revision
        if proposal_id is not UNSET:
            field_dict["proposal_id"] = proposal_id
        if cascade_from is not UNSET:
            field_dict["cascade_from"] = cascade_from

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _origin = d.pop("origin", UNSET)
        origin: MemoryEntityProposalOriginOrigin | Unset
        if isinstance(_origin, Unset):
            origin = UNSET
        else:
            origin = MemoryEntityProposalOriginOrigin(_origin)

        actor = d.pop("actor", UNSET)

        reason = d.pop("reason", UNSET)

        created_at_ms = d.pop("created_at_ms", UNSET)

        revision = d.pop("revision", UNSET)

        def _parse_proposal_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        proposal_id = _parse_proposal_id(d.pop("proposal_id", UNSET))

        def _parse_cascade_from(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        cascade_from = _parse_cascade_from(d.pop("cascade_from", UNSET))

        memory_entity_proposal_origin = cls(
            origin=origin,
            actor=actor,
            reason=reason,
            created_at_ms=created_at_ms,
            revision=revision,
            proposal_id=proposal_id,
            cascade_from=cascade_from,
        )

        return memory_entity_proposal_origin
