from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.constrained_output import ConstrainedOutput
from ..models.constrained_probabilities import ConstrainedProbabilities
from ..models.generated_decide_request_decision_method import GeneratedDecideRequestDecisionMethod
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.constrained_generation_batch import ConstrainedGenerationBatch
    from ..models.decision_input import DecisionInput
    from ..models.generated_choice_decision_question import GeneratedChoiceDecisionQuestion
    from ..models.generated_multi_choice_decision_question import GeneratedMultiChoiceDecisionQuestion
    from ..models.generated_predicate_decision_question import GeneratedPredicateDecisionQuestion
    from ..models.generated_score_decision_question import GeneratedScoreDecisionQuestion
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="GeneratedDecideRequest")


@_attrs_define
class GeneratedDecideRequest:
    """Supply exactly one of input or inputs. Each question requires instructions unless nonempty decider instructions are
    supplied. Inference nodes admit registered native generators; hosted execution uses configured producer and SQL
    runtimes.

        Attributes:
            decision_method (GeneratedDecideRequestDecisionMethod):
            generator (GeneratorConfig): A unified configuration for a generative AI provider.
                 Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            questions (list[GeneratedChoiceDecisionQuestion | GeneratedMultiChoiceDecisionQuestion |
                GeneratedPredicateDecisionQuestion | GeneratedScoreDecisionQuestion]):
            input_ (str | Unset):
            inputs (list[DecisionInput] | Unset):
            instructions (str | Unset):
            output (ConstrainedOutput | Unset): Provider and model admission selects a supported constrained mechanism. Auto
                prefers forced tools, then provider JSON schema, then native grammar. Free-text parsing is not a mode.
            probabilities (ConstrainedProbabilities | Unset): Token probabilities require complete decoder evidence for the
                allowed values. Verdict-only is explicit opt-in; model-reported confidence is never used.
            batch (ConstrainedGenerationBatch | Unset):
    """

    decision_method: GeneratedDecideRequestDecisionMethod
    generator: GeneratorConfig
    questions: list[
        GeneratedChoiceDecisionQuestion
        | GeneratedMultiChoiceDecisionQuestion
        | GeneratedPredicateDecisionQuestion
        | GeneratedScoreDecisionQuestion
    ]
    input_: str | Unset = UNSET
    inputs: list[DecisionInput] | Unset = UNSET
    instructions: str | Unset = UNSET
    output: ConstrainedOutput | Unset = UNSET
    probabilities: ConstrainedProbabilities | Unset = UNSET
    batch: ConstrainedGenerationBatch | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.generated_choice_decision_question import GeneratedChoiceDecisionQuestion
        from ..models.generated_multi_choice_decision_question import GeneratedMultiChoiceDecisionQuestion
        from ..models.generated_predicate_decision_question import GeneratedPredicateDecisionQuestion

        decision_method = self.decision_method.value

        generator = self.generator.to_dict()

        questions = []
        for questions_item_data in self.questions:
            questions_item: dict[str, Any]
            if isinstance(questions_item_data, GeneratedChoiceDecisionQuestion):
                questions_item = questions_item_data.to_dict()
            elif isinstance(questions_item_data, GeneratedMultiChoiceDecisionQuestion):
                questions_item = questions_item_data.to_dict()
            elif isinstance(questions_item_data, GeneratedPredicateDecisionQuestion):
                questions_item = questions_item_data.to_dict()
            else:
                questions_item = questions_item_data.to_dict()

            questions.append(questions_item)

        input_ = self.input_

        inputs: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.inputs, Unset):
            inputs = []
            for inputs_item_data in self.inputs:
                inputs_item = inputs_item_data.to_dict()
                inputs.append(inputs_item)

        instructions = self.instructions

        output: str | Unset = UNSET
        if not isinstance(self.output, Unset):
            output = self.output.value

        probabilities: str | Unset = UNSET
        if not isinstance(self.probabilities, Unset):
            probabilities = self.probabilities.value

        batch: dict[str, Any] | Unset = UNSET
        if not isinstance(self.batch, Unset):
            batch = self.batch.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "decision_method": decision_method,
                "generator": generator,
                "questions": questions,
            }
        )
        if input_ is not UNSET:
            field_dict["input"] = input_
        if inputs is not UNSET:
            field_dict["inputs"] = inputs
        if instructions is not UNSET:
            field_dict["instructions"] = instructions
        if output is not UNSET:
            field_dict["output"] = output
        if probabilities is not UNSET:
            field_dict["probabilities"] = probabilities
        if batch is not UNSET:
            field_dict["batch"] = batch

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.constrained_generation_batch import ConstrainedGenerationBatch
        from ..models.decision_input import DecisionInput
        from ..models.generated_choice_decision_question import GeneratedChoiceDecisionQuestion
        from ..models.generated_multi_choice_decision_question import GeneratedMultiChoiceDecisionQuestion
        from ..models.generated_predicate_decision_question import GeneratedPredicateDecisionQuestion
        from ..models.generated_score_decision_question import GeneratedScoreDecisionQuestion
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        decision_method = GeneratedDecideRequestDecisionMethod(d.pop("decision_method"))

        generator = GeneratorConfig.from_dict(d.pop("generator"))

        questions = []
        _questions = d.pop("questions")
        for questions_item_data in _questions:

            def _parse_questions_item(
                data: object,
            ) -> (
                GeneratedChoiceDecisionQuestion
                | GeneratedMultiChoiceDecisionQuestion
                | GeneratedPredicateDecisionQuestion
                | GeneratedScoreDecisionQuestion
            ):
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_generated_decide_question_type_0 = GeneratedChoiceDecisionQuestion.from_dict(data)

                    return componentsschemas_generated_decide_question_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_generated_decide_question_type_1 = GeneratedMultiChoiceDecisionQuestion.from_dict(
                        data
                    )

                    return componentsschemas_generated_decide_question_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_generated_decide_question_type_2 = GeneratedPredicateDecisionQuestion.from_dict(
                        data
                    )

                    return componentsschemas_generated_decide_question_type_2
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_generated_decide_question_type_3 = GeneratedScoreDecisionQuestion.from_dict(data)

                return componentsschemas_generated_decide_question_type_3

            questions_item = _parse_questions_item(questions_item_data)

            questions.append(questions_item)

        input_ = d.pop("input", UNSET)

        _inputs = d.pop("inputs", UNSET)
        inputs: list[DecisionInput] | Unset = UNSET
        if _inputs is not UNSET:
            inputs = []
            for inputs_item_data in _inputs:
                inputs_item = DecisionInput.from_dict(inputs_item_data)

                inputs.append(inputs_item)

        instructions = d.pop("instructions", UNSET)

        _output = d.pop("output", UNSET)
        output: ConstrainedOutput | Unset
        if isinstance(_output, Unset):
            output = UNSET
        else:
            output = ConstrainedOutput(_output)

        _probabilities = d.pop("probabilities", UNSET)
        probabilities: ConstrainedProbabilities | Unset
        if isinstance(_probabilities, Unset):
            probabilities = UNSET
        else:
            probabilities = ConstrainedProbabilities(_probabilities)

        _batch = d.pop("batch", UNSET)
        batch: ConstrainedGenerationBatch | Unset
        if isinstance(_batch, Unset):
            batch = UNSET
        else:
            batch = ConstrainedGenerationBatch.from_dict(_batch)

        generated_decide_request = cls(
            decision_method=decision_method,
            generator=generator,
            questions=questions,
            input_=input_,
            inputs=inputs,
            instructions=instructions,
            output=output,
            probabilities=probabilities,
            batch=batch,
        )

        return generated_decide_request
