from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.classification_step_config import ClassificationStepConfig
    from ..models.confidence_step_config import ConfidenceStepConfig
    from ..models.followup_step_config import FollowupStepConfig
    from ..models.generation_step_config import GenerationStepConfig


T = TypeVar("T", bound="AnswerAgentSteps")


@_attrs_define
class AnswerAgentSteps:
    """DEPRECATED: Use RetrievalAgentSteps instead.
    Configuration for the answer agent's pipeline steps.

        Attributes:
            classification (Union[Unset, ClassificationStepConfig]): Configuration for the classification step. This step
                analyzes the query,
                selects the optimal retrieval strategy, and generates semantic transformations.
            answer (Union[Unset, GenerationStepConfig]): Configuration for the generation step. This step generates the
                final
                response from retrieved documents using the reasoning as context.
            followup (Union[Unset, FollowupStepConfig]): Configuration for generating follow-up questions. Uses a separate
                generator
                call which can use a cheaper/faster model.
            confidence (Union[Unset, ConfidenceStepConfig]): Configuration for confidence assessment. Evaluates answer
                quality and
                resource relevance. Can use a model calibrated for scoring tasks.
    """

    classification: Union[Unset, "ClassificationStepConfig"] = UNSET
    answer: Union[Unset, "GenerationStepConfig"] = UNSET
    followup: Union[Unset, "FollowupStepConfig"] = UNSET
    confidence: Union[Unset, "ConfidenceStepConfig"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        classification: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.classification, Unset):
            classification = self.classification.to_dict()

        answer: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.answer, Unset):
            answer = self.answer.to_dict()

        followup: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.followup, Unset):
            followup = self.followup.to_dict()

        confidence: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.confidence, Unset):
            confidence = self.confidence.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if classification is not UNSET:
            field_dict["classification"] = classification
        if answer is not UNSET:
            field_dict["answer"] = answer
        if followup is not UNSET:
            field_dict["followup"] = followup
        if confidence is not UNSET:
            field_dict["confidence"] = confidence

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.classification_step_config import ClassificationStepConfig
        from ..models.confidence_step_config import ConfidenceStepConfig
        from ..models.followup_step_config import FollowupStepConfig
        from ..models.generation_step_config import GenerationStepConfig

        d = dict(src_dict)
        _classification = d.pop("classification", UNSET)
        classification: Union[Unset, ClassificationStepConfig]
        if isinstance(_classification, Unset):
            classification = UNSET
        else:
            classification = ClassificationStepConfig.from_dict(_classification)

        _answer = d.pop("answer", UNSET)
        answer: Union[Unset, GenerationStepConfig]
        if isinstance(_answer, Unset):
            answer = UNSET
        else:
            answer = GenerationStepConfig.from_dict(_answer)

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

        answer_agent_steps = cls(
            classification=classification,
            answer=answer,
            followup=followup,
            confidence=confidence,
        )

        answer_agent_steps.additional_properties = d
        return answer_agent_steps

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
