from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.multi_choice_decision_question_type import MultiChoiceDecisionQuestionType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_choice import DecisionChoice
    from ..models.embedding_decision_acceptance import EmbeddingDecisionAcceptance
    from ..models.multi_choice_decision_question_similarity_thresholds_type_1 import (
        MultiChoiceDecisionQuestionSimilarityThresholdsType1,
    )


T = TypeVar("T", bound="MultiChoiceDecisionQuestion")


@_attrs_define
class MultiChoiceDecisionQuestion:
    """Antfly multi-label extension for embedding similarity; selected, empty and abstained are distinct results.

    Attributes:
        type_ (MultiChoiceDecisionQuestionType):
        name (str):
        instructions (str):
        choices (list[DecisionChoice]):
        embedding_options (EmbeddingDecisionAcceptance | Unset): Question-specific acceptance policy for embedding
            similarity. Trained decision models reject it.
        similarity_thresholds (float | MultiChoiceDecisionQuestionSimilarityThresholdsType1 | Unset): Required raw
            cosine cutoff for every choice, either one common cutoff or a complete choice-to-cutoff map. Alternatively
            supply a qualified question-specific calibration_id. Mutually exclusive with calibration_id. Scores equal to
            their cutoff are selected.
    """

    type_: MultiChoiceDecisionQuestionType
    name: str
    instructions: str
    choices: list[DecisionChoice]
    embedding_options: EmbeddingDecisionAcceptance | Unset = UNSET
    similarity_thresholds: float | MultiChoiceDecisionQuestionSimilarityThresholdsType1 | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.multi_choice_decision_question_similarity_thresholds_type_1 import (
            MultiChoiceDecisionQuestionSimilarityThresholdsType1,
        )

        type_ = self.type_.value

        name = self.name

        instructions = self.instructions

        choices = []
        for choices_item_data in self.choices:
            choices_item = choices_item_data.to_dict()
            choices.append(choices_item)

        embedding_options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding_options, Unset):
            embedding_options = self.embedding_options.to_dict()

        similarity_thresholds: dict[str, Any] | float | Unset
        if isinstance(self.similarity_thresholds, Unset):
            similarity_thresholds = UNSET
        elif isinstance(self.similarity_thresholds, MultiChoiceDecisionQuestionSimilarityThresholdsType1):
            similarity_thresholds = self.similarity_thresholds.to_dict()
        else:
            similarity_thresholds = self.similarity_thresholds

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "name": name,
                "instructions": instructions,
                "choices": choices,
            }
        )
        if embedding_options is not UNSET:
            field_dict["embedding_options"] = embedding_options
        if similarity_thresholds is not UNSET:
            field_dict["similarity_thresholds"] = similarity_thresholds

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_choice import DecisionChoice
        from ..models.embedding_decision_acceptance import EmbeddingDecisionAcceptance
        from ..models.multi_choice_decision_question_similarity_thresholds_type_1 import (
            MultiChoiceDecisionQuestionSimilarityThresholdsType1,
        )

        d = dict(src_dict)
        type_ = MultiChoiceDecisionQuestionType(d.pop("type"))

        name = d.pop("name")

        instructions = d.pop("instructions")

        choices = []
        _choices = d.pop("choices")
        for choices_item_data in _choices:
            choices_item = DecisionChoice.from_dict(choices_item_data)

            choices.append(choices_item)

        _embedding_options = d.pop("embedding_options", UNSET)
        embedding_options: EmbeddingDecisionAcceptance | Unset
        if isinstance(_embedding_options, Unset):
            embedding_options = UNSET
        else:
            embedding_options = EmbeddingDecisionAcceptance.from_dict(_embedding_options)

        def _parse_similarity_thresholds(
            data: object,
        ) -> float | MultiChoiceDecisionQuestionSimilarityThresholdsType1 | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                similarity_thresholds_type_1 = MultiChoiceDecisionQuestionSimilarityThresholdsType1.from_dict(data)

                return similarity_thresholds_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(float | MultiChoiceDecisionQuestionSimilarityThresholdsType1 | Unset, data)

        similarity_thresholds = _parse_similarity_thresholds(d.pop("similarity_thresholds", UNSET))

        multi_choice_decision_question = cls(
            type_=type_,
            name=name,
            instructions=instructions,
            choices=choices,
            embedding_options=embedding_options,
            similarity_thresholds=similarity_thresholds,
        )

        return multi_choice_decision_question
