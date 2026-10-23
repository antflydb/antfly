from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_claim_pin import MemoryClaimPin


T = TypeVar("T", bound="MemorySupersessionProposalRequest")


@_attrs_define
class MemorySupersessionProposalRequest:
    """
    Attributes:
        id (str):
        expected_revision (int):
        incoming (MemoryClaimPin):
        previous (MemoryClaimPin):
        actor (str | Unset):
    """

    id: str
    expected_revision: int
    incoming: MemoryClaimPin
    previous: MemoryClaimPin
    actor: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        expected_revision = self.expected_revision

        incoming = self.incoming.to_dict()

        previous = self.previous.to_dict()

        actor = self.actor

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "expected_revision": expected_revision,
                "incoming": incoming,
                "previous": previous,
            }
        )
        if actor is not UNSET:
            field_dict["actor"] = actor

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_claim_pin import MemoryClaimPin

        d = dict(src_dict)
        id = d.pop("id")

        expected_revision = d.pop("expected_revision")

        incoming = MemoryClaimPin.from_dict(d.pop("incoming"))

        previous = MemoryClaimPin.from_dict(d.pop("previous"))

        actor = d.pop("actor", UNSET)

        memory_supersession_proposal_request = cls(
            id=id,
            expected_revision=expected_revision,
            incoming=incoming,
            previous=previous,
            actor=actor,
        )

        return memory_supersession_proposal_request
