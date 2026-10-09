from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.embedding_choice_answer import EmbeddingChoiceAnswer
    from ..models.embedding_multi_choice_answer import EmbeddingMultiChoiceAnswer
    from ..models.trained_choice_answer import TrainedChoiceAnswer
    from ..models.trained_predicate_answer import TrainedPredicateAnswer
    from ..models.trained_score_answer import TrainedScoreAnswer


T = TypeVar("T", bound="DecisionBatchItem")


@_attrs_define
class DecisionBatchItem:
    """
    Attributes:
        input_index (int):
        answers (list[EmbeddingChoiceAnswer | EmbeddingMultiChoiceAnswer | TrainedChoiceAnswer | TrainedPredicateAnswer
            | TrainedScoreAnswer]):
        id (str | Unset):
    """

    input_index: int
    answers: list[
        EmbeddingChoiceAnswer
        | EmbeddingMultiChoiceAnswer
        | TrainedChoiceAnswer
        | TrainedPredicateAnswer
        | TrainedScoreAnswer
    ]
    id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.embedding_choice_answer import EmbeddingChoiceAnswer
        from ..models.trained_choice_answer import TrainedChoiceAnswer
        from ..models.trained_predicate_answer import TrainedPredicateAnswer
        from ..models.trained_score_answer import TrainedScoreAnswer

        input_index = self.input_index

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

        id = self.id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "input_index": input_index,
                "answers": answers,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.embedding_choice_answer import EmbeddingChoiceAnswer
        from ..models.embedding_multi_choice_answer import EmbeddingMultiChoiceAnswer
        from ..models.trained_choice_answer import TrainedChoiceAnswer
        from ..models.trained_predicate_answer import TrainedPredicateAnswer
        from ..models.trained_score_answer import TrainedScoreAnswer

        d = dict(src_dict)
        input_index = d.pop("input_index")

        answers = []
        _answers = d.pop("answers")
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

        id = d.pop("id", UNSET)

        decision_batch_item = cls(
            input_index=input_index,
            answers=answers,
            id=id,
        )

        return decision_batch_item
