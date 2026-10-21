from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.curation_proposal_decision_result_atomicity import CurationProposalDecisionResultAtomicity
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.curation_proposal import CurationProposal
    from ..models.curation_proposal_decision_result_results_item import CurationProposalDecisionResultResultsItem


T = TypeVar("T", bound="CurationProposalDecisionResult")


@_attrs_define
class CurationProposalDecisionResult:
    """
    Attributes:
        proposal (CurationProposal | Unset):
        complete (bool | Unset):
        changed (bool | Unset):
        atomicity (CurationProposalDecisionResultAtomicity | Unset):
        applied (bool | Unset):
        results (list[CurationProposalDecisionResultResultsItem] | Unset):
    """

    proposal: CurationProposal | Unset = UNSET
    complete: bool | Unset = UNSET
    changed: bool | Unset = UNSET
    atomicity: CurationProposalDecisionResultAtomicity | Unset = UNSET
    applied: bool | Unset = UNSET
    results: list[CurationProposalDecisionResultResultsItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        proposal: dict[str, Any] | Unset = UNSET
        if not isinstance(self.proposal, Unset):
            proposal = self.proposal.to_dict()

        complete = self.complete

        changed = self.changed

        atomicity: str | Unset = UNSET
        if not isinstance(self.atomicity, Unset):
            atomicity = self.atomicity.value

        applied = self.applied

        results: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.results, Unset):
            results = []
            for results_item_data in self.results:
                results_item = results_item_data.to_dict()
                results.append(results_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if proposal is not UNSET:
            field_dict["proposal"] = proposal
        if complete is not UNSET:
            field_dict["complete"] = complete
        if changed is not UNSET:
            field_dict["changed"] = changed
        if atomicity is not UNSET:
            field_dict["atomicity"] = atomicity
        if applied is not UNSET:
            field_dict["applied"] = applied
        if results is not UNSET:
            field_dict["results"] = results

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.curation_proposal import CurationProposal
        from ..models.curation_proposal_decision_result_results_item import CurationProposalDecisionResultResultsItem

        d = dict(src_dict)
        _proposal = d.pop("proposal", UNSET)
        proposal: CurationProposal | Unset
        if isinstance(_proposal, Unset):
            proposal = UNSET
        else:
            proposal = CurationProposal.from_dict(_proposal)

        complete = d.pop("complete", UNSET)

        changed = d.pop("changed", UNSET)

        _atomicity = d.pop("atomicity", UNSET)
        atomicity: CurationProposalDecisionResultAtomicity | Unset
        if isinstance(_atomicity, Unset):
            atomicity = UNSET
        else:
            atomicity = CurationProposalDecisionResultAtomicity(_atomicity)

        applied = d.pop("applied", UNSET)

        _results = d.pop("results", UNSET)
        results: list[CurationProposalDecisionResultResultsItem] | Unset = UNSET
        if _results is not UNSET:
            results = []
            for results_item_data in _results:
                results_item = CurationProposalDecisionResultResultsItem.from_dict(results_item_data)

                results.append(results_item)

        curation_proposal_decision_result = cls(
            proposal=proposal,
            complete=complete,
            changed=changed,
            atomicity=atomicity,
            applied=applied,
            results=results,
        )

        curation_proposal_decision_result.additional_properties = d
        return curation_proposal_decision_result

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
