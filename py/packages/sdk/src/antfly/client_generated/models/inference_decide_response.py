from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_batch_item import DecisionBatchItem
    from ..models.decision_usage import DecisionUsage
    from ..models.embedding_choice_answer import EmbeddingChoiceAnswer
    from ..models.embedding_multi_choice_answer import EmbeddingMultiChoiceAnswer
    from ..models.trained_choice_answer import TrainedChoiceAnswer
    from ..models.trained_predicate_answer import TrainedPredicateAnswer
    from ..models.trained_score_answer import TrainedScoreAnswer


T = TypeVar("T", bound="InferenceDecideResponse")


@_attrs_define
class InferenceDecideResponse:
    """Single-input responses have answers. Batch responses have data in input order, with one named answer array per input
    and aggregate usage.

        Attributes:
            model (str):
            usage (DecisionUsage):
            model_identity (str | Unset):
            renderer_version (str | Unset):
            answers (list[EmbeddingChoiceAnswer | EmbeddingMultiChoiceAnswer | TrainedChoiceAnswer | TrainedPredicateAnswer
                | TrainedScoreAnswer] | Unset):
            data (list[DecisionBatchItem] | Unset):
    """

    model: str
    usage: DecisionUsage
    model_identity: str | Unset = UNSET
    renderer_version: str | Unset = UNSET
    answers: (
        list[
            EmbeddingChoiceAnswer
            | EmbeddingMultiChoiceAnswer
            | TrainedChoiceAnswer
            | TrainedPredicateAnswer
            | TrainedScoreAnswer
        ]
        | Unset
    ) = UNSET
    data: list[DecisionBatchItem] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.embedding_choice_answer import EmbeddingChoiceAnswer
        from ..models.trained_choice_answer import TrainedChoiceAnswer
        from ..models.trained_predicate_answer import TrainedPredicateAnswer
        from ..models.trained_score_answer import TrainedScoreAnswer

        model = self.model

        usage = self.usage.to_dict()

        model_identity = self.model_identity

        renderer_version = self.renderer_version

        answers: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.answers, Unset):
            answers = []
            for answers_item_data in self.answers:
                answers_item: dict[str, Any]
                if isinstance(answers_item_data, TrainedChoiceAnswer):
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

        data: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.data, Unset):
            data = []
            for data_item_data in self.data:
                data_item = data_item_data.to_dict()
                data.append(data_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "model": model,
                "usage": usage,
            }
        )
        if model_identity is not UNSET:
            field_dict["model_identity"] = model_identity
        if renderer_version is not UNSET:
            field_dict["renderer_version"] = renderer_version
        if answers is not UNSET:
            field_dict["answers"] = answers
        if data is not UNSET:
            field_dict["data"] = data

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_batch_item import DecisionBatchItem
        from ..models.decision_usage import DecisionUsage
        from ..models.embedding_choice_answer import EmbeddingChoiceAnswer
        from ..models.embedding_multi_choice_answer import EmbeddingMultiChoiceAnswer
        from ..models.trained_choice_answer import TrainedChoiceAnswer
        from ..models.trained_predicate_answer import TrainedPredicateAnswer
        from ..models.trained_score_answer import TrainedScoreAnswer

        d = dict(src_dict)
        model = d.pop("model")

        usage = DecisionUsage.from_dict(d.pop("usage"))

        model_identity = d.pop("model_identity", UNSET)

        renderer_version = d.pop("renderer_version", UNSET)

        _answers = d.pop("answers", UNSET)
        answers: (
            list[
                EmbeddingChoiceAnswer
                | EmbeddingMultiChoiceAnswer
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
                    | TrainedChoiceAnswer
                    | TrainedPredicateAnswer
                    | TrainedScoreAnswer
                ):
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_0 = TrainedChoiceAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_1 = TrainedScoreAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_1
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_2 = TrainedPredicateAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_2
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_inference_decide_answer_type_3 = EmbeddingChoiceAnswer.from_dict(data)

                        return componentsschemas_inference_decide_answer_type_3
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_inference_decide_answer_type_4 = EmbeddingMultiChoiceAnswer.from_dict(data)

                    return componentsschemas_inference_decide_answer_type_4

                answers_item = _parse_answers_item(answers_item_data)

                answers.append(answers_item)

        _data = d.pop("data", UNSET)
        data: list[DecisionBatchItem] | Unset = UNSET
        if _data is not UNSET:
            data = []
            for data_item_data in _data:
                data_item = DecisionBatchItem.from_dict(data_item_data)

                data.append(data_item)

        inference_decide_response = cls(
            model=model,
            usage=usage,
            model_identity=model_identity,
            renderer_version=renderer_version,
            answers=answers,
            data=data,
        )

        return inference_decide_response
