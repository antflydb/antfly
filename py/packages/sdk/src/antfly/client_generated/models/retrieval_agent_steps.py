from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.classification_step_config import ClassificationStepConfig
    from ..models.confidence_step_config import ConfidenceStepConfig
    from ..models.eval_config import EvalConfig
    from ..models.followup_step_config import FollowupStepConfig
    from ..models.generation_step_config import GenerationStepConfig
    from ..models.retrieval_step_config import RetrievalStepConfig


T = TypeVar("T", bound="RetrievalAgentSteps")


@_attrs_define
class RetrievalAgentSteps:
    """Configuration for the retrieval agent's pipeline steps and tool-use behavior.
    Each step can have its own generator (or chain of generators) and step-specific options.
    If a step is not configured, it is skipped (retrieval always runs).

        Attributes:
            classification (ClassificationStepConfig | Unset): Configuration for the classification step. This step analyzes
                the query,
                selects the optimal retrieval strategy, and generates semantic transformations.
            retrieval (RetrievalStepConfig | Unset): Configuration for the retrieval step. Retrieval tools are constrained
                by
                the top-level request tools policy when both are present.
            generation (GenerationStepConfig | Unset): Configuration for the generation step. This step generates the final
                response from retrieved documents using the reasoning as context.
            followup (FollowupStepConfig | Unset): Configuration for deterministic follow-up suggestions derived from the
                original query and the standard Antfly follow-up templates.
            confidence (ConfidenceStepConfig | Unset): Configuration for confidence assessment. Evaluates answer quality and
                resource relevance. Can use a model calibrated for scoring tasks.
            eval_ (EvalConfig | Unset): Configuration for inline evaluation of query results.
                Add to RetrievalAgentRequest, QueryRequest, or other evaluation-capable request schemas.
    """

    classification: ClassificationStepConfig | Unset = UNSET
    retrieval: RetrievalStepConfig | Unset = UNSET
    generation: GenerationStepConfig | Unset = UNSET
    followup: FollowupStepConfig | Unset = UNSET
    confidence: ConfidenceStepConfig | Unset = UNSET
    eval_: EvalConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        classification: dict[str, Any] | Unset = UNSET
        if not isinstance(self.classification, Unset):
            classification = self.classification.to_dict()

        retrieval: dict[str, Any] | Unset = UNSET
        if not isinstance(self.retrieval, Unset):
            retrieval = self.retrieval.to_dict()

        generation: dict[str, Any] | Unset = UNSET
        if not isinstance(self.generation, Unset):
            generation = self.generation.to_dict()

        followup: dict[str, Any] | Unset = UNSET
        if not isinstance(self.followup, Unset):
            followup = self.followup.to_dict()

        confidence: dict[str, Any] | Unset = UNSET
        if not isinstance(self.confidence, Unset):
            confidence = self.confidence.to_dict()

        eval_: dict[str, Any] | Unset = UNSET
        if not isinstance(self.eval_, Unset):
            eval_ = self.eval_.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if classification is not UNSET:
            field_dict["classification"] = classification
        if retrieval is not UNSET:
            field_dict["retrieval"] = retrieval
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
        from ..models.classification_step_config import ClassificationStepConfig
        from ..models.confidence_step_config import ConfidenceStepConfig
        from ..models.eval_config import EvalConfig
        from ..models.followup_step_config import FollowupStepConfig
        from ..models.generation_step_config import GenerationStepConfig
        from ..models.retrieval_step_config import RetrievalStepConfig

        d = dict(src_dict)
        _classification = d.pop("classification", UNSET)
        classification: ClassificationStepConfig | Unset
        if isinstance(_classification, Unset):
            classification = UNSET
        else:
            classification = ClassificationStepConfig.from_dict(_classification)

        _retrieval = d.pop("retrieval", UNSET)
        retrieval: RetrievalStepConfig | Unset
        if isinstance(_retrieval, Unset):
            retrieval = UNSET
        else:
            retrieval = RetrievalStepConfig.from_dict(_retrieval)

        _generation = d.pop("generation", UNSET)
        generation: GenerationStepConfig | Unset
        if isinstance(_generation, Unset):
            generation = UNSET
        else:
            generation = GenerationStepConfig.from_dict(_generation)

        _followup = d.pop("followup", UNSET)
        followup: FollowupStepConfig | Unset
        if isinstance(_followup, Unset):
            followup = UNSET
        else:
            followup = FollowupStepConfig.from_dict(_followup)

        _confidence = d.pop("confidence", UNSET)
        confidence: ConfidenceStepConfig | Unset
        if isinstance(_confidence, Unset):
            confidence = UNSET
        else:
            confidence = ConfidenceStepConfig.from_dict(_confidence)

        _eval_ = d.pop("eval", UNSET)
        eval_: EvalConfig | Unset
        if isinstance(_eval_, Unset):
            eval_ = UNSET
        else:
            eval_ = EvalConfig.from_dict(_eval_)

        retrieval_agent_steps = cls(
            classification=classification,
            retrieval=retrieval,
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
