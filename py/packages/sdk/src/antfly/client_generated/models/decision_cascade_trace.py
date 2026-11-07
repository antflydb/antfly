from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.decision_cascade_agreement_status import DecisionCascadeAgreementStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_cascade_stage_trace import DecisionCascadeStageTrace


T = TypeVar("T", bound="DecisionCascadeTrace")


@_attrs_define
class DecisionCascadeTrace:
    """Present when a cascade decider answered. Lists the stages that ran for this input, in order. The top-level model and
    answers come from the answering stage, and usage is the sum over these stages.

        Attributes:
            decider (str): The configured cascade name.
            definition_sha256 (str): Identity of the cascade definition, excluding credentials. Any change to stages, bands,
                failure policy or budget changes it.
            answered_by (str): Name of the stage whose answer is final.
            stages (list[DecisionCascadeStageTrace]):
            agreement_status (DecisionCascadeAgreementStatus | Unset): A required comparison agreed, disagreed, or could not
                run because its stage failed with keep_previous. Disagreed and unverified outcomes abstain without discarding
                model answers or usage.
            agreed (bool | Unset): Derived comparison result. Present only for agreed or disagreed checks; absent when no
                check ran or verification was unavailable.
            disagreed_questions (list[str] | Unset): Sorted question names that disagreed. Present only when
                agreement_status is disagreed.
    """

    decider: str
    definition_sha256: str
    answered_by: str
    stages: list[DecisionCascadeStageTrace]
    agreement_status: DecisionCascadeAgreementStatus | Unset = UNSET
    agreed: bool | Unset = UNSET
    disagreed_questions: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        decider = self.decider

        definition_sha256 = self.definition_sha256

        answered_by = self.answered_by

        stages = []
        for stages_item_data in self.stages:
            stages_item = stages_item_data.to_dict()
            stages.append(stages_item)

        agreement_status: str | Unset = UNSET
        if not isinstance(self.agreement_status, Unset):
            agreement_status = self.agreement_status.value

        agreed = self.agreed

        disagreed_questions: list[str] | Unset = UNSET
        if not isinstance(self.disagreed_questions, Unset):
            disagreed_questions = self.disagreed_questions

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "decider": decider,
                "definition_sha256": definition_sha256,
                "answered_by": answered_by,
                "stages": stages,
            }
        )
        if agreement_status is not UNSET:
            field_dict["agreement_status"] = agreement_status
        if agreed is not UNSET:
            field_dict["agreed"] = agreed
        if disagreed_questions is not UNSET:
            field_dict["disagreed_questions"] = disagreed_questions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_cascade_stage_trace import DecisionCascadeStageTrace

        d = dict(src_dict)
        decider = d.pop("decider")

        definition_sha256 = d.pop("definition_sha256")

        answered_by = d.pop("answered_by")

        stages = []
        _stages = d.pop("stages")
        for stages_item_data in _stages:
            stages_item = DecisionCascadeStageTrace.from_dict(stages_item_data)

            stages.append(stages_item)

        _agreement_status = d.pop("agreement_status", UNSET)
        agreement_status: DecisionCascadeAgreementStatus | Unset
        if isinstance(_agreement_status, Unset):
            agreement_status = UNSET
        else:
            agreement_status = DecisionCascadeAgreementStatus(_agreement_status)

        agreed = d.pop("agreed", UNSET)

        disagreed_questions = cast(list[str], d.pop("disagreed_questions", UNSET))

        decision_cascade_trace = cls(
            decider=decider,
            definition_sha256=definition_sha256,
            answered_by=answered_by,
            stages=stages,
            agreement_status=agreement_status,
            agreed=agreed,
            disagreed_questions=disagreed_questions,
        )

        return decision_cascade_trace
