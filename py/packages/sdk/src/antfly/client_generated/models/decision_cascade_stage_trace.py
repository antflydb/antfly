from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.decision_cascade_agreement_status import DecisionCascadeAgreementStatus
from ..models.decision_cascade_stage_trace_decision_method import DecisionCascadeStageTraceDecisionMethod
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_cascade_stage_trace_provenance import DecisionCascadeStageTraceProvenance
    from ..models.decision_cascade_upstream_agreement import DecisionCascadeUpstreamAgreement
    from ..models.decision_revision_dependency import DecisionRevisionDependency
    from ..models.decision_usage import DecisionUsage
    from ..models.embedding_choice_answer import EmbeddingChoiceAnswer
    from ..models.embedding_multi_choice_answer import EmbeddingMultiChoiceAnswer
    from ..models.generated_choice_answer import GeneratedChoiceAnswer
    from ..models.generated_multi_choice_answer import GeneratedMultiChoiceAnswer
    from ..models.generated_predicate_answer import GeneratedPredicateAnswer
    from ..models.generated_score_answer import GeneratedScoreAnswer
    from ..models.trained_choice_answer import TrainedChoiceAnswer
    from ..models.trained_predicate_answer import TrainedPredicateAnswer
    from ..models.trained_score_answer import TrainedScoreAnswer


T = TypeVar("T", bound="DecisionCascadeStageTrace")


@_attrs_define
class DecisionCascadeStageTrace:
    """
    Attributes:
        name (str):
        decision_method (DecisionCascadeStageTraceDecisionMethod):
        escalated (bool): Whether this answer fell inside the next stage's band, sending the input on.
        agreement_questions (list[str] | Unset): Sorted question names compared by this stage. Present only when a local
            comparison ran. Allows upstream consumers to verify scoped agreement directly from the stage answers without
            reconstructing its policy. Historical traces may omit this field; ambiguous historical agreement abstains.
        upstream_revisions (list[DecisionRevisionDependency] | Unset): Complete bounded upstream execution dependencies.
            An empty array explicitly records no versioned dependencies. Omission on historical projected proxy stages
            indicates incomplete provenance.
        upstream_agreement (DecisionCascadeUpstreamAgreement | Unset): Bounded agreement summary reported by the stage's
            upstream provider. This preserves upstream abstention without recursively embedding its trace; the upstream
            definition is not locally verified. A disagreed or unverified upstream stops the input, including at the first
            or an unguarded stage.
        agreement_status (DecisionCascadeAgreementStatus | Unset): A required comparison agreed, disagreed, or could not
            run because its stage failed with keep_previous or its upstream abstained. Disagreed and unverified outcomes
            abstain without discarding model answers or usage.
        agreed (bool | Unset): Derived comparison result. Present only for agreed or disagreed checks; absent when no
            check ran or verification was unavailable.
        disagreed_questions (list[str] | Unset): Sorted question names that disagreed. Present only when
            agreement_status is disagreed.
        model (str | Unset):
        model_revision (str | Unset): Immutable model revision reported by this stage, when available.
        answers (list[EmbeddingChoiceAnswer | EmbeddingMultiChoiceAnswer | GeneratedChoiceAnswer |
            GeneratedMultiChoiceAnswer | GeneratedPredicateAnswer | GeneratedScoreAnswer | TrainedChoiceAnswer |
            TrainedPredicateAnswer | TrainedScoreAnswer] | Unset): This stage's answers. Absent when the stage failed.
        usage (DecisionUsage | Unset):
        provenance (DecisionCascadeStageTraceProvenance | Unset): Producer provenance reported for this stage, when
            present.
        error (str | Unset): Error code when this stage failed for the input. With on_failure keep_previous, the
            previous stage's answer stands.
    """

    name: str
    decision_method: DecisionCascadeStageTraceDecisionMethod
    escalated: bool
    agreement_questions: list[str] | Unset = UNSET
    upstream_revisions: list[DecisionRevisionDependency] | Unset = UNSET
    upstream_agreement: DecisionCascadeUpstreamAgreement | Unset = UNSET
    agreement_status: DecisionCascadeAgreementStatus | Unset = UNSET
    agreed: bool | Unset = UNSET
    disagreed_questions: list[str] | Unset = UNSET
    model: str | Unset = UNSET
    model_revision: str | Unset = UNSET
    answers: (
        list[
            EmbeddingChoiceAnswer
            | EmbeddingMultiChoiceAnswer
            | GeneratedChoiceAnswer
            | GeneratedMultiChoiceAnswer
            | GeneratedPredicateAnswer
            | GeneratedScoreAnswer
            | TrainedChoiceAnswer
            | TrainedPredicateAnswer
            | TrainedScoreAnswer
        ]
        | Unset
    ) = UNSET
    usage: DecisionUsage | Unset = UNSET
    provenance: DecisionCascadeStageTraceProvenance | Unset = UNSET
    error: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.embedding_choice_answer import EmbeddingChoiceAnswer
        from ..models.generated_choice_answer import GeneratedChoiceAnswer
        from ..models.generated_multi_choice_answer import GeneratedMultiChoiceAnswer
        from ..models.generated_predicate_answer import GeneratedPredicateAnswer
        from ..models.generated_score_answer import GeneratedScoreAnswer
        from ..models.trained_choice_answer import TrainedChoiceAnswer
        from ..models.trained_predicate_answer import TrainedPredicateAnswer
        from ..models.trained_score_answer import TrainedScoreAnswer

        name = self.name

        decision_method = self.decision_method.value

        escalated = self.escalated

        agreement_questions: list[str] | Unset = UNSET
        if not isinstance(self.agreement_questions, Unset):
            agreement_questions = self.agreement_questions

        upstream_revisions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.upstream_revisions, Unset):
            upstream_revisions = []
            for upstream_revisions_item_data in self.upstream_revisions:
                upstream_revisions_item = upstream_revisions_item_data.to_dict()
                upstream_revisions.append(upstream_revisions_item)

        upstream_agreement: dict[str, Any] | Unset = UNSET
        if not isinstance(self.upstream_agreement, Unset):
            upstream_agreement = self.upstream_agreement.to_dict()

        agreement_status: str | Unset = UNSET
        if not isinstance(self.agreement_status, Unset):
            agreement_status = self.agreement_status.value

        agreed = self.agreed

        disagreed_questions: list[str] | Unset = UNSET
        if not isinstance(self.disagreed_questions, Unset):
            disagreed_questions = self.disagreed_questions

        model = self.model

        model_revision = self.model_revision

        answers: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.answers, Unset):
            answers = []
            for answers_item_data in self.answers:
                answers_item: dict[str, Any]
                if isinstance(answers_item_data, GeneratedChoiceAnswer):
                    answers_item = answers_item_data.to_dict()
                elif isinstance(answers_item_data, GeneratedMultiChoiceAnswer):
                    answers_item = answers_item_data.to_dict()
                elif isinstance(answers_item_data, GeneratedPredicateAnswer):
                    answers_item = answers_item_data.to_dict()
                elif isinstance(answers_item_data, GeneratedScoreAnswer):
                    answers_item = answers_item_data.to_dict()
                elif isinstance(answers_item_data, TrainedChoiceAnswer):
                    answers_item = answers_item_data.to_dict()
                elif isinstance(answers_item_data, TrainedScoreAnswer):
                    answers_item = answers_item_data.to_dict()
                elif isinstance(answers_item_data, TrainedPredicateAnswer):
                    answers_item = answers_item_data.to_dict()
                elif isinstance(answers_item_data, EmbeddingChoiceAnswer):
                    answers_item = answers_item_data.to_dict()
                else:
                    answers_item = answers_item_data.to_dict()

                answers.append(answers_item)

        usage: dict[str, Any] | Unset = UNSET
        if not isinstance(self.usage, Unset):
            usage = self.usage.to_dict()

        provenance: dict[str, Any] | Unset = UNSET
        if not isinstance(self.provenance, Unset):
            provenance = self.provenance.to_dict()

        error = self.error

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "decision_method": decision_method,
                "escalated": escalated,
            }
        )
        if agreement_questions is not UNSET:
            field_dict["agreement_questions"] = agreement_questions
        if upstream_revisions is not UNSET:
            field_dict["upstream_revisions"] = upstream_revisions
        if upstream_agreement is not UNSET:
            field_dict["upstream_agreement"] = upstream_agreement
        if agreement_status is not UNSET:
            field_dict["agreement_status"] = agreement_status
        if agreed is not UNSET:
            field_dict["agreed"] = agreed
        if disagreed_questions is not UNSET:
            field_dict["disagreed_questions"] = disagreed_questions
        if model is not UNSET:
            field_dict["model"] = model
        if model_revision is not UNSET:
            field_dict["model_revision"] = model_revision
        if answers is not UNSET:
            field_dict["answers"] = answers
        if usage is not UNSET:
            field_dict["usage"] = usage
        if provenance is not UNSET:
            field_dict["provenance"] = provenance
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_cascade_stage_trace_provenance import DecisionCascadeStageTraceProvenance
        from ..models.decision_cascade_upstream_agreement import DecisionCascadeUpstreamAgreement
        from ..models.decision_revision_dependency import DecisionRevisionDependency
        from ..models.decision_usage import DecisionUsage
        from ..models.embedding_choice_answer import EmbeddingChoiceAnswer
        from ..models.embedding_multi_choice_answer import EmbeddingMultiChoiceAnswer
        from ..models.generated_choice_answer import GeneratedChoiceAnswer
        from ..models.generated_multi_choice_answer import GeneratedMultiChoiceAnswer
        from ..models.generated_predicate_answer import GeneratedPredicateAnswer
        from ..models.generated_score_answer import GeneratedScoreAnswer
        from ..models.trained_choice_answer import TrainedChoiceAnswer
        from ..models.trained_predicate_answer import TrainedPredicateAnswer
        from ..models.trained_score_answer import TrainedScoreAnswer

        d = dict(src_dict)
        name = d.pop("name")

        decision_method = DecisionCascadeStageTraceDecisionMethod(d.pop("decision_method"))

        escalated = d.pop("escalated")

        agreement_questions = cast(list[str], d.pop("agreement_questions", UNSET))

        _upstream_revisions = d.pop("upstream_revisions", UNSET)
        upstream_revisions: list[DecisionRevisionDependency] | Unset = UNSET
        if _upstream_revisions is not UNSET:
            upstream_revisions = []
            for upstream_revisions_item_data in _upstream_revisions:
                upstream_revisions_item = DecisionRevisionDependency.from_dict(upstream_revisions_item_data)

                upstream_revisions.append(upstream_revisions_item)

        _upstream_agreement = d.pop("upstream_agreement", UNSET)
        upstream_agreement: DecisionCascadeUpstreamAgreement | Unset
        if isinstance(_upstream_agreement, Unset):
            upstream_agreement = UNSET
        else:
            upstream_agreement = DecisionCascadeUpstreamAgreement.from_dict(_upstream_agreement)

        _agreement_status = d.pop("agreement_status", UNSET)
        agreement_status: DecisionCascadeAgreementStatus | Unset
        if isinstance(_agreement_status, Unset):
            agreement_status = UNSET
        else:
            agreement_status = DecisionCascadeAgreementStatus(_agreement_status)

        agreed = d.pop("agreed", UNSET)

        disagreed_questions = cast(list[str], d.pop("disagreed_questions", UNSET))

        model = d.pop("model", UNSET)

        model_revision = d.pop("model_revision", UNSET)

        _answers = d.pop("answers", UNSET)
        answers: (
            list[
                EmbeddingChoiceAnswer
                | EmbeddingMultiChoiceAnswer
                | GeneratedChoiceAnswer
                | GeneratedMultiChoiceAnswer
                | GeneratedPredicateAnswer
                | GeneratedScoreAnswer
                | TrainedChoiceAnswer
                | TrainedPredicateAnswer
                | TrainedScoreAnswer
            ]
            | Unset
        ) = UNSET
        if _answers is not UNSET:
            answers = []
            for answers_item_data in _answers:

                def _parse_answers_item(
                    data: object,
                ) -> (
                    EmbeddingChoiceAnswer
                    | EmbeddingMultiChoiceAnswer
                    | GeneratedChoiceAnswer
                    | GeneratedMultiChoiceAnswer
                    | GeneratedPredicateAnswer
                    | GeneratedScoreAnswer
                    | TrainedChoiceAnswer
                    | TrainedPredicateAnswer
                    | TrainedScoreAnswer
                ):
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_0 = GeneratedChoiceAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_1 = GeneratedMultiChoiceAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_1
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_2 = GeneratedPredicateAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_2
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_3 = GeneratedScoreAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_3
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_4 = TrainedChoiceAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_4
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_5 = TrainedScoreAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_5
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_6 = TrainedPredicateAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_6
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_7 = EmbeddingChoiceAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_7
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_inference_decide_answer_type_8 = EmbeddingMultiChoiceAnswer.from_dict(data)

                    return componentsschemas_inference_decide_answer_type_8

                answers_item = _parse_answers_item(answers_item_data)

                answers.append(answers_item)

        _usage = d.pop("usage", UNSET)
        usage: DecisionUsage | Unset
        if isinstance(_usage, Unset):
            usage = UNSET
        else:
            usage = DecisionUsage.from_dict(_usage)

        _provenance = d.pop("provenance", UNSET)
        provenance: DecisionCascadeStageTraceProvenance | Unset
        if isinstance(_provenance, Unset):
            provenance = UNSET
        else:
            provenance = DecisionCascadeStageTraceProvenance.from_dict(_provenance)

        error = d.pop("error", UNSET)

        decision_cascade_stage_trace = cls(
            name=name,
            decision_method=decision_method,
            escalated=escalated,
            agreement_questions=agreement_questions,
            upstream_revisions=upstream_revisions,
            upstream_agreement=upstream_agreement,
            agreement_status=agreement_status,
            agreed=agreed,
            disagreed_questions=disagreed_questions,
            model=model,
            model_revision=model_revision,
            answers=answers,
            usage=usage,
            provenance=provenance,
            error=error,
        )

        return decision_cascade_stage_trace
