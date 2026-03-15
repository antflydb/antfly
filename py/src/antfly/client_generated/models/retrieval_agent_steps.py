from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chat_tools_config import ChatToolsConfig
    from ..models.classification_step_config import ClassificationStepConfig
    from ..models.confidence_step_config import ConfidenceStepConfig
    from ..models.eval_config import EvalConfig
    from ..models.followup_step_config import FollowupStepConfig
    from ..models.generation_step_config import GenerationStepConfig


T = TypeVar("T", bound="RetrievalAgentSteps")


@_attrs_define
class RetrievalAgentSteps:
    """Configuration for the retrieval agent's pipeline steps and tool-use behavior.
    Each step can have its own generator (or chain of generators) and step-specific options.
    If a step is not configured, it is skipped (retrieval always runs).

        Attributes:
            tools (Union[Unset, ChatToolsConfig]): Configuration for chat agent tools.

                If `enabled_tools` is empty/omitted, defaults to: add_filter, ask_clarification, search.

                For models that don't support native tool calling (e.g., Ollama),
                a prompt-based fallback is used with structured output parsing.
            classification (Union[Unset, ClassificationStepConfig]): Configuration for the classification step. This step
                analyzes the query,
                selects the optimal retrieval strategy, and generates semantic transformations.
            generation (Union[Unset, GenerationStepConfig]): Configuration for the generation step. This step generates the
                final
                response from retrieved documents using the reasoning as context.
            followup (Union[Unset, FollowupStepConfig]): Configuration for generating follow-up questions. Uses a separate
                generator
                call which can use a cheaper/faster model.
            confidence (Union[Unset, ConfidenceStepConfig]): Configuration for confidence assessment. Evaluates answer
                quality and
                resource relevance. Can use a model calibrated for scoring tasks.
            eval_ (Union[Unset, EvalConfig]): Configuration for inline evaluation of query results.
                Add to RAGRequest, QueryRequest, or AnswerAgentRequest.
    """

    tools: Union[Unset, "ChatToolsConfig"] = UNSET
    classification: Union[Unset, "ClassificationStepConfig"] = UNSET
    generation: Union[Unset, "GenerationStepConfig"] = UNSET
    followup: Union[Unset, "FollowupStepConfig"] = UNSET
    confidence: Union[Unset, "ConfidenceStepConfig"] = UNSET
    eval_: Union[Unset, "EvalConfig"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        tools: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.tools, Unset):
            tools = self.tools.to_dict()

        classification: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.classification, Unset):
            classification = self.classification.to_dict()

        generation: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.generation, Unset):
            generation = self.generation.to_dict()

        followup: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.followup, Unset):
            followup = self.followup.to_dict()

        confidence: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.confidence, Unset):
            confidence = self.confidence.to_dict()

        eval_: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.eval_, Unset):
            eval_ = self.eval_.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if tools is not UNSET:
            field_dict["tools"] = tools
        if classification is not UNSET:
            field_dict["classification"] = classification
        if generation is not UNSET:
            field_dict["generation"] = generation
        if followup is not UNSET:
            field_dict["followup"] = followup
        if confidence is not UNSET:
            field_dict["confidence"] = confidence
        if eval_ is not UNSET:
            field_dict["eval"] = eval_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chat_tools_config import ChatToolsConfig
        from ..models.classification_step_config import ClassificationStepConfig
        from ..models.confidence_step_config import ConfidenceStepConfig
        from ..models.eval_config import EvalConfig
        from ..models.followup_step_config import FollowupStepConfig
        from ..models.generation_step_config import GenerationStepConfig

        d = dict(src_dict)
        _tools = d.pop("tools", UNSET)
        tools: Union[Unset, ChatToolsConfig]
        if isinstance(_tools, Unset):
            tools = UNSET
        else:
            tools = ChatToolsConfig.from_dict(_tools)

        _classification = d.pop("classification", UNSET)
        classification: Union[Unset, ClassificationStepConfig]
        if isinstance(_classification, Unset):
            classification = UNSET
        else:
            classification = ClassificationStepConfig.from_dict(_classification)

        _generation = d.pop("generation", UNSET)
        generation: Union[Unset, GenerationStepConfig]
        if isinstance(_generation, Unset):
            generation = UNSET
        else:
            generation = GenerationStepConfig.from_dict(_generation)

        _followup = d.pop("followup", UNSET)
        followup: Union[Unset, FollowupStepConfig]
        if isinstance(_followup, Unset):
            followup = UNSET
        else:
            followup = FollowupStepConfig.from_dict(_followup)

        _confidence = d.pop("confidence", UNSET)
        confidence: Union[Unset, ConfidenceStepConfig]
        if isinstance(_confidence, Unset):
            confidence = UNSET
        else:
            confidence = ConfidenceStepConfig.from_dict(_confidence)

        _eval_ = d.pop("eval", UNSET)
        eval_: Union[Unset, EvalConfig]
        if isinstance(_eval_, Unset):
            eval_ = UNSET
        else:
            eval_ = EvalConfig.from_dict(_eval_)

        retrieval_agent_steps = cls(
            tools=tools,
            classification=classification,
            generation=generation,
            followup=followup,
            confidence=confidence,
            eval_=eval_,
        )

        retrieval_agent_steps.additional_properties = d
        return retrieval_agent_steps

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
