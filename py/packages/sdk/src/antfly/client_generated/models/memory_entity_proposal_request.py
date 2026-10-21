from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_entity_proposal_request_absorbed import MemoryEntityProposalRequestAbsorbed
from ..models.memory_entity_proposal_request_action import MemoryEntityProposalRequestAction
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity_proposal_request_evidence_item import MemoryEntityProposalRequestEvidenceItem


T = TypeVar("T", bound="MemoryEntityProposalRequest")


@_attrs_define
class MemoryEntityProposalRequest:
    """
    Attributes:
        action (MemoryEntityProposalRequestAction):
        from_ (str):
        expected_revision (int):
        to (None | str | Unset):
        replace (bool | Unset):  Default: False.
        pin_distinct (bool | Unset):  Default: False.
        absorbed (MemoryEntityProposalRequestAbsorbed | Unset):  Default: MemoryEntityProposalRequestAbsorbed.REJECT.
        actor (str | Unset):
        evidence (list[MemoryEntityProposalRequestEvidenceItem] | Unset):
    """

    action: MemoryEntityProposalRequestAction
    from_: str
    expected_revision: int
    to: None | str | Unset = UNSET
    replace: bool | Unset = False
    pin_distinct: bool | Unset = False
    absorbed: MemoryEntityProposalRequestAbsorbed | Unset = MemoryEntityProposalRequestAbsorbed.REJECT
    actor: str | Unset = UNSET
    evidence: list[MemoryEntityProposalRequestEvidenceItem] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        action = self.action.value

        from_ = self.from_

        expected_revision = self.expected_revision

        to: None | str | Unset
        if isinstance(self.to, Unset):
            to = UNSET
        else:
            to = self.to

        replace = self.replace

        pin_distinct = self.pin_distinct

        absorbed: str | Unset = UNSET
        if not isinstance(self.absorbed, Unset):
            absorbed = self.absorbed.value

        actor = self.actor

        evidence: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.evidence, Unset):
            evidence = []
            for evidence_item_data in self.evidence:
                evidence_item = evidence_item_data.to_dict()
                evidence.append(evidence_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "action": action,
                "from": from_,
                "expected_revision": expected_revision,
            }
        )
        if to is not UNSET:
            field_dict["to"] = to
        if replace is not UNSET:
            field_dict["replace"] = replace
        if pin_distinct is not UNSET:
            field_dict["pin_distinct"] = pin_distinct
        if absorbed is not UNSET:
            field_dict["absorbed"] = absorbed
        if actor is not UNSET:
            field_dict["actor"] = actor
        if evidence is not UNSET:
            field_dict["evidence"] = evidence

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_proposal_request_evidence_item import MemoryEntityProposalRequestEvidenceItem

        d = dict(src_dict)
        action = MemoryEntityProposalRequestAction(d.pop("action"))

        from_ = d.pop("from")

        expected_revision = d.pop("expected_revision")

        def _parse_to(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        to = _parse_to(d.pop("to", UNSET))

        replace = d.pop("replace", UNSET)

        pin_distinct = d.pop("pin_distinct", UNSET)

        _absorbed = d.pop("absorbed", UNSET)
        absorbed: MemoryEntityProposalRequestAbsorbed | Unset
        if isinstance(_absorbed, Unset):
            absorbed = UNSET
        else:
            absorbed = MemoryEntityProposalRequestAbsorbed(_absorbed)

        actor = d.pop("actor", UNSET)

        _evidence = d.pop("evidence", UNSET)
        evidence: list[MemoryEntityProposalRequestEvidenceItem] | Unset = UNSET
        if _evidence is not UNSET:
            evidence = []
            for evidence_item_data in _evidence:
                evidence_item = MemoryEntityProposalRequestEvidenceItem.from_dict(evidence_item_data)

                evidence.append(evidence_item)

        memory_entity_proposal_request = cls(
            action=action,
            from_=from_,
            expected_revision=expected_revision,
            to=to,
            replace=replace,
            pin_distinct=pin_distinct,
            absorbed=absorbed,
            actor=actor,
            evidence=evidence,
        )

        return memory_entity_proposal_request
