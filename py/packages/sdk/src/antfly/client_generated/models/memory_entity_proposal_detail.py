from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_entity_proposal import MemoryEntityProposal
    from ..models.memory_entity_proposal_inputs import MemoryEntityProposalInputs


T = TypeVar("T", bound="MemoryEntityProposalDetail")


@_attrs_define
class MemoryEntityProposalDetail:
    """
    Attributes:
        proposal (MemoryEntityProposal):
        inputs (MemoryEntityProposalInputs):
        reason (None | str):
        stale_configuration (bool):
        revision (int):
    """

    proposal: MemoryEntityProposal
    inputs: MemoryEntityProposalInputs
    reason: None | str
    stale_configuration: bool
    revision: int

    def to_dict(self) -> dict[str, Any]:
        proposal = self.proposal.to_dict()

        inputs = self.inputs.to_dict()

        reason: None | str
        reason = self.reason

        stale_configuration = self.stale_configuration

        revision = self.revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "proposal": proposal,
                "inputs": inputs,
                "reason": reason,
                "stale_configuration": stale_configuration,
                "revision": revision,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_proposal import MemoryEntityProposal
        from ..models.memory_entity_proposal_inputs import MemoryEntityProposalInputs

        d = dict(src_dict)
        proposal = MemoryEntityProposal.from_dict(d.pop("proposal"))

        inputs = MemoryEntityProposalInputs.from_dict(d.pop("inputs"))

        def _parse_reason(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        reason = _parse_reason(d.pop("reason"))

        stale_configuration = d.pop("stale_configuration")

        revision = d.pop("revision")

        memory_entity_proposal_detail = cls(
            proposal=proposal,
            inputs=inputs,
            reason=reason,
            stale_configuration=stale_configuration,
            revision=revision,
        )

        return memory_entity_proposal_detail
