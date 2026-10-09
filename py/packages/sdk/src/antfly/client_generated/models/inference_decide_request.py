from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.choice_decision_question import ChoiceDecisionQuestion
    from ..models.decision_input import DecisionInput
    from ..models.embedding_decision_options import EmbeddingDecisionOptions
    from ..models.inference_decide_long_document import InferenceDecideLongDocument
    from ..models.multi_choice_decision_question import MultiChoiceDecisionQuestion
    from ..models.predicate_decision_question import PredicateDecisionQuestion
    from ..models.score_decision_question import ScoreDecisionQuestion


T = TypeVar("T", bound="InferenceDecideRequest")


@_attrs_define
class InferenceDecideRequest:
    """Supply exactly one of input (single text) or inputs (Antfly batch extension). Question names must be unique. Single
    text and named question arrays follow OpenAI Decisions conventions; local models support text input and string
    choice values. Request bodies are bounded at 16 MiB.

        Attributes:
            model (str):
            questions (list[ChoiceDecisionQuestion | MultiChoiceDecisionQuestion | PredicateDecisionQuestion |
                ScoreDecisionQuestion]):
            model_identity (str | Unset): Pin the exact EmbeddingGemma 2 assets and embedding recipe.
            input_ (str | Unset):
            inputs (list[DecisionInput] | Unset):
            long_document (InferenceDecideLongDocument | Unset): Explicit windowing for qualified boundary decision models.
                Span and embedding decision models reject window mode. Omission preserves rejection of over-limit text.
            embedding_options (EmbeddingDecisionOptions | Unset): Request-wide embedding geometry defaults. Acceptance
                thresholds and calibration belong to individual questions.
    """

    model: str
    questions: list[
        ChoiceDecisionQuestion | MultiChoiceDecisionQuestion | PredicateDecisionQuestion | ScoreDecisionQuestion
    ]
    model_identity: str | Unset = UNSET
    input_: str | Unset = UNSET
    inputs: list[DecisionInput] | Unset = UNSET
    long_document: InferenceDecideLongDocument | Unset = UNSET
    embedding_options: EmbeddingDecisionOptions | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.choice_decision_question import ChoiceDecisionQuestion
        from ..models.multi_choice_decision_question import MultiChoiceDecisionQuestion
        from ..models.score_decision_question import ScoreDecisionQuestion

        model = self.model

        questions = []
        for questions_item_data in self.questions:
            questions_item: dict[str, Any]
            if isinstance(questions_item_data, ChoiceDecisionQuestion):
                questions_item = questions_item_data.to_dict()
            elif isinstance(questions_item_data, MultiChoiceDecisionQuestion):
                questions_item = questions_item_data.to_dict()
            elif isinstance(questions_item_data, ScoreDecisionQuestion):
                questions_item = questions_item_data.to_dict()
            else:
                questions_item = questions_item_data.to_dict()

            questions.append(questions_item)

        model_identity = self.model_identity

        input_ = self.input_

        inputs: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.inputs, Unset):
            inputs = []
            for inputs_item_data in self.inputs:
                inputs_item = inputs_item_data.to_dict()
                inputs.append(inputs_item)

        long_document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.long_document, Unset):
            long_document = self.long_document.to_dict()

        embedding_options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding_options, Unset):
            embedding_options = self.embedding_options.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "model": model,
                "questions": questions,
            }
        )
        if model_identity is not UNSET:
            field_dict["model_identity"] = model_identity
        if input_ is not UNSET:
            field_dict["input"] = input_
        if inputs is not UNSET:
            field_dict["inputs"] = inputs
        if long_document is not UNSET:
            field_dict["long_document"] = long_document
        if embedding_options is not UNSET:
            field_dict["embedding_options"] = embedding_options

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.choice_decision_question import ChoiceDecisionQuestion
        from ..models.decision_input import DecisionInput
        from ..models.embedding_decision_options import EmbeddingDecisionOptions
        from ..models.inference_decide_long_document import InferenceDecideLongDocument
        from ..models.multi_choice_decision_question import MultiChoiceDecisionQuestion
        from ..models.predicate_decision_question import PredicateDecisionQuestion
        from ..models.score_decision_question import ScoreDecisionQuestion

        d = dict(src_dict)
        model = d.pop("model")

        questions = []
        _questions = d.pop("questions")
        for questions_item_data in _questions:

            def _parse_questions_item(
                data: object,
            ) -> (
                ChoiceDecisionQuestion | MultiChoiceDecisionQuestion | PredicateDecisionQuestion | ScoreDecisionQuestion
            ):
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_inference_decide_question_type_0 = ChoiceDecisionQuestion.from_dict(data)

                    return componentsschemas_inference_decide_question_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_inference_decide_question_type_1 = MultiChoiceDecisionQuestion.from_dict(data)

                    return componentsschemas_inference_decide_question_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_inference_decide_question_type_2 = ScoreDecisionQuestion.from_dict(data)

                    return componentsschemas_inference_decide_question_type_2
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_inference_decide_question_type_3 = PredicateDecisionQuestion.from_dict(data)

                return componentsschemas_inference_decide_question_type_3

            questions_item = _parse_questions_item(questions_item_data)

            questions.append(questions_item)

        model_identity = d.pop("model_identity", UNSET)

        input_ = d.pop("input", UNSET)

        _inputs = d.pop("inputs", UNSET)
        inputs: list[DecisionInput] | Unset = UNSET
        if _inputs is not UNSET:
            inputs = []
            for inputs_item_data in _inputs:
                inputs_item = DecisionInput.from_dict(inputs_item_data)

                inputs.append(inputs_item)

        _long_document = d.pop("long_document", UNSET)
        long_document: InferenceDecideLongDocument | Unset
        if isinstance(_long_document, Unset):
            long_document = UNSET
        else:
            long_document = InferenceDecideLongDocument.from_dict(_long_document)

        _embedding_options = d.pop("embedding_options", UNSET)
        embedding_options: EmbeddingDecisionOptions | Unset
        if isinstance(_embedding_options, Unset):
            embedding_options = UNSET
        else:
            embedding_options = EmbeddingDecisionOptions.from_dict(_embedding_options)

        inference_decide_request = cls(
            model=model,
            questions=questions,
            model_identity=model_identity,
            input_=input_,
            inputs=inputs,
            long_document=long_document,
            embedding_options=embedding_options,
        )

        return inference_decide_request
