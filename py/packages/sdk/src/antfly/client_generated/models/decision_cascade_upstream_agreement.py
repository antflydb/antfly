from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.decision_cascade_agreement_status import DecisionCascadeAgreementStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="DecisionCascadeUpstreamAgreement")


@_attrs_define
class DecisionCascadeUpstreamAgreement:
    """Bounded agreement summary reported by the stage's upstream provider. This preserves upstream abstention without
    recursively embedding its trace; the upstream definition is not locally verified. A disagreed or unverified upstream
    stops the input, including at the first or an unguarded stage.

        Attributes:
            agreement_status (DecisionCascadeAgreementStatus): A required comparison agreed, disagreed, or could not run
                because its stage failed with keep_previous or its upstream abstained. Disagreed and unverified outcomes abstain
                without discarding model answers or usage.
            agreed (bool | Unset): True for agreed, false for disagreed, absent for unverified.
            disagreed_questions (list[str] | Unset): Sorted names from this stage's answers; required only for disagreed.
            decider (str | Unset):
            definition_sha256 (str | Unset):
            answered_by (str | Unset):
    """

    agreement_status: DecisionCascadeAgreementStatus
    agreed: bool | Unset = UNSET
    disagreed_questions: list[str] | Unset = UNSET
    decider: str | Unset = UNSET
    definition_sha256: str | Unset = UNSET
    answered_by: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        agreement_status = self.agreement_status.value

        agreed = self.agreed

        disagreed_questions: list[str] | Unset = UNSET
        if not isinstance(self.disagreed_questions, Unset):
            disagreed_questions = self.disagreed_questions

        decider = self.decider

        definition_sha256 = self.definition_sha256

        answered_by = self.answered_by

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "agreement_status": agreement_status,
            }
        )
        if agreed is not UNSET:
            field_dict["agreed"] = agreed
        if disagreed_questions is not UNSET:
            field_dict["disagreed_questions"] = disagreed_questions
        if decider is not UNSET:
            field_dict["decider"] = decider
        if definition_sha256 is not UNSET:
            field_dict["definition_sha256"] = definition_sha256
        if answered_by is not UNSET:
            field_dict["answered_by"] = answered_by

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        agreement_status = DecisionCascadeAgreementStatus(d.pop("agreement_status"))

        agreed = d.pop("agreed", UNSET)

        disagreed_questions = cast(list[str], d.pop("disagreed_questions", UNSET))

        decider = d.pop("decider", UNSET)

        definition_sha256 = d.pop("definition_sha256", UNSET)

        answered_by = d.pop("answered_by", UNSET)

        decision_cascade_upstream_agreement = cls(
            agreement_status=agreement_status,
            agreed=agreed,
            disagreed_questions=disagreed_questions,
            decider=decider,
            definition_sha256=definition_sha256,
            answered_by=answered_by,
        )

        return decision_cascade_upstream_agreement
