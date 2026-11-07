from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.decision_cascade_stage_trace_decision_method import DecisionCascadeStageTraceDecisionMethod
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_cascade_stage_trace_provenance import DecisionCascadeStageTraceProvenance
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
            model=model,
            model_revision=model_revision,
            answers=answers,
            usage=usage,
            provenance=provenance,
            error=error,
        )

        return decision_cascade_stage_trace
