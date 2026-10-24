from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_claim_view import MemoryClaimView
    from ..models.memory_supersession_proposal import MemorySupersessionProposal


T = TypeVar("T", bound="MemorySupersessionProposalDetail")


@_attrs_define
class MemorySupersessionProposalDetail:
    """
    Attributes:
        proposal (MemorySupersessionProposal):
        incoming (MemoryClaimView):
        previous (MemoryClaimView):
        stale_configuration (bool):
    """

    proposal: MemorySupersessionProposal
    incoming: MemoryClaimView
    previous: MemoryClaimView
    stale_configuration: bool

    def to_dict(self) -> dict[str, Any]:
        proposal = self.proposal.to_dict()

        incoming = self.incoming.to_dict()

        previous = self.previous.to_dict()

        stale_configuration = self.stale_configuration

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "proposal": proposal,
                "incoming": incoming,
                "previous": previous,
                "stale_configuration": stale_configuration,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_claim_view import MemoryClaimView
        from ..models.memory_supersession_proposal import MemorySupersessionProposal

        d = dict(src_dict)
        proposal = MemorySupersessionProposal.from_dict(d.pop("proposal"))

        incoming = MemoryClaimView.from_dict(d.pop("incoming"))

        previous = MemoryClaimView.from_dict(d.pop("previous"))

        stale_configuration = d.pop("stale_configuration")

        memory_supersession_proposal_detail = cls(
            proposal=proposal,
            incoming=incoming,
            previous=previous,
            stale_configuration=stale_configuration,
        )

        return memory_supersession_proposal_detail
