from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.embedding_multi_choice_answer_abstention_reason import EmbeddingMultiChoiceAnswerAbstentionReason
from ..models.embedding_multi_choice_answer_decision_method import EmbeddingMultiChoiceAnswerDecisionMethod
from ..models.embedding_multi_choice_answer_similarity_metric import EmbeddingMultiChoiceAnswerSimilarityMetric
from ..models.embedding_multi_choice_answer_status import EmbeddingMultiChoiceAnswerStatus
from ..models.embedding_multi_choice_answer_type import EmbeddingMultiChoiceAnswerType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_similarity import DecisionSimilarity
    from ..models.embedding_multi_choice_answer_similarity_thresholds import (
        EmbeddingMultiChoiceAnswerSimilarityThresholds,
    )


T = TypeVar("T", bound="EmbeddingMultiChoiceAnswer")


@_attrs_define
class EmbeddingMultiChoiceAnswer:
    """Raw cosine scores are neither probabilities nor confidence. Empty means no label met its threshold; abstained means
    the requested margin could not be established.

        Attributes:
            name (str):
            type_ (EmbeddingMultiChoiceAnswerType):
            decision_method (EmbeddingMultiChoiceAnswerDecisionMethod):
            similarity_metric (EmbeddingMultiChoiceAnswerSimilarityMetric): Cosine is fixed for this embedding recipe and
                its threshold artifacts. Index distance settings are independent.
            similarities (list[DecisionSimilarity]):
            margin (float):
            status (EmbeddingMultiChoiceAnswerStatus):
            prototype_set_hash (str):
            choices (list[str]):
            similarity_thresholds (EmbeddingMultiChoiceAnswerSimilarityThresholds): Effective cosine selection threshold for
                every choice, including fitted calibration thresholds.
            calibration_id (str | Unset):
            abstention_reason (EmbeddingMultiChoiceAnswerAbstentionReason | Unset):
    """

    name: str
    type_: EmbeddingMultiChoiceAnswerType
    decision_method: EmbeddingMultiChoiceAnswerDecisionMethod
    similarity_metric: EmbeddingMultiChoiceAnswerSimilarityMetric
    similarities: list[DecisionSimilarity]
    margin: float
    status: EmbeddingMultiChoiceAnswerStatus
    prototype_set_hash: str
    choices: list[str]
    similarity_thresholds: EmbeddingMultiChoiceAnswerSimilarityThresholds
    calibration_id: str | Unset = UNSET
    abstention_reason: EmbeddingMultiChoiceAnswerAbstentionReason | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        decision_method = self.decision_method.value

        similarity_metric = self.similarity_metric.value

        similarities = []
        for similarities_item_data in self.similarities:
            similarities_item = similarities_item_data.to_dict()
            similarities.append(similarities_item)

        margin = self.margin

        status = self.status.value

        prototype_set_hash = self.prototype_set_hash

        choices = self.choices

        similarity_thresholds = self.similarity_thresholds.to_dict()

        calibration_id = self.calibration_id

        abstention_reason: str | Unset = UNSET
        if not isinstance(self.abstention_reason, Unset):
            abstention_reason = self.abstention_reason.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "type": type_,
                "decision_method": decision_method,
                "similarity_metric": similarity_metric,
                "similarities": similarities,
                "margin": margin,
                "status": status,
                "prototype_set_hash": prototype_set_hash,
                "choices": choices,
                "similarity_thresholds": similarity_thresholds,
            }
        )
        if calibration_id is not UNSET:
            field_dict["calibration_id"] = calibration_id
        if abstention_reason is not UNSET:
            field_dict["abstention_reason"] = abstention_reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_similarity import DecisionSimilarity
        from ..models.embedding_multi_choice_answer_similarity_thresholds import (
            EmbeddingMultiChoiceAnswerSimilarityThresholds,
        )

        d = dict(src_dict)
        name = d.pop("name")

        type_ = EmbeddingMultiChoiceAnswerType(d.pop("type"))

        decision_method = EmbeddingMultiChoiceAnswerDecisionMethod(d.pop("decision_method"))

        similarity_metric = EmbeddingMultiChoiceAnswerSimilarityMetric(d.pop("similarity_metric"))

        similarities = []
        _similarities = d.pop("similarities")
        for similarities_item_data in _similarities:
            similarities_item = DecisionSimilarity.from_dict(similarities_item_data)

            similarities.append(similarities_item)

        margin = d.pop("margin")

        status = EmbeddingMultiChoiceAnswerStatus(d.pop("status"))

        prototype_set_hash = d.pop("prototype_set_hash")

        choices = cast(list[str], d.pop("choices"))

        similarity_thresholds = EmbeddingMultiChoiceAnswerSimilarityThresholds.from_dict(d.pop("similarity_thresholds"))

        calibration_id = d.pop("calibration_id", UNSET)

        _abstention_reason = d.pop("abstention_reason", UNSET)
        abstention_reason: EmbeddingMultiChoiceAnswerAbstentionReason | Unset
        if isinstance(_abstention_reason, Unset):
            abstention_reason = UNSET
        else:
            abstention_reason = EmbeddingMultiChoiceAnswerAbstentionReason(_abstention_reason)

        embedding_multi_choice_answer = cls(
            name=name,
            type_=type_,
            decision_method=decision_method,
            similarity_metric=similarity_metric,
            similarities=similarities,
            margin=margin,
            status=status,
            prototype_set_hash=prototype_set_hash,
            choices=choices,
            similarity_thresholds=similarity_thresholds,
            calibration_id=calibration_id,
            abstention_reason=abstention_reason,
        )

        return embedding_multi_choice_answer
