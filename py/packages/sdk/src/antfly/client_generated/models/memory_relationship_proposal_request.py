from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_relationship_proposal_request_assertion import MemoryRelationshipProposalRequestAssertion


T = TypeVar("T", bound="MemoryRelationshipProposalRequest")


@_attrs_define
class MemoryRelationshipProposalRequest:
    """
    Attributes:
        id (str):
        expected_revision (int):
        assertion (MemoryRelationshipProposalRequestAssertion):
        actor (str | Unset):
    """

    id: str
    expected_revision: int
    assertion: MemoryRelationshipProposalRequestAssertion
    actor: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        expected_revision = self.expected_revision

        assertion = self.assertion.to_dict()

        actor = self.actor

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "expected_revision": expected_revision,
                "assertion": assertion,
            }
        )
        if actor is not UNSET:
            field_dict["actor"] = actor

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_relationship_proposal_request_assertion import MemoryRelationshipProposalRequestAssertion

        d = dict(src_dict)
        id = d.pop("id")

        expected_revision = d.pop("expected_revision")

        assertion = MemoryRelationshipProposalRequestAssertion.from_dict(d.pop("assertion"))

        actor = d.pop("actor", UNSET)

        memory_relationship_proposal_request = cls(
            id=id,
            expected_revision=expected_revision,
            assertion=assertion,
            actor=actor,
        )

        return memory_relationship_proposal_request
