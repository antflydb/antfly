from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.embedding_choice_answer_abstention_reason import EmbeddingChoiceAnswerAbstentionReason
from ..models.embedding_choice_answer_decision_method import EmbeddingChoiceAnswerDecisionMethod
from ..models.embedding_choice_answer_similarity_metric import EmbeddingChoiceAnswerSimilarityMetric
from ..models.embedding_choice_answer_status import EmbeddingChoiceAnswerStatus
from ..models.embedding_choice_answer_type import EmbeddingChoiceAnswerType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_similarity import DecisionSimilarity


T = TypeVar("T", bound="EmbeddingChoiceAnswer")


@_attrs_define
class EmbeddingChoiceAnswer:
    """Raw cosine scores are neither probabilities nor confidence. Empty means no label met its threshold; abstained means
    the requested margin could not be established.

        Attributes:
            name (str):
            type_ (EmbeddingChoiceAnswerType):
            decision_method (EmbeddingChoiceAnswerDecisionMethod):
            similarity_metric (EmbeddingChoiceAnswerSimilarityMetric): Cosine is fixed for this embedding recipe and its
                threshold artifacts. Index distance settings are independent.
            similarities (list[DecisionSimilarity]):
            margin (float):
            status (EmbeddingChoiceAnswerStatus):
            prototype_set_hash (str):
            choice (None | str): Selected identifier, or null on abstention.
            calibration_id (str | Unset):
            abstention_reason (EmbeddingChoiceAnswerAbstentionReason | Unset):
    """

    name: str
    type_: EmbeddingChoiceAnswerType
    decision_method: EmbeddingChoiceAnswerDecisionMethod
    similarity_metric: EmbeddingChoiceAnswerSimilarityMetric
    similarities: list[DecisionSimilarity]
    margin: float
    status: EmbeddingChoiceAnswerStatus
    prototype_set_hash: str
    choice: None | str
    calibration_id: str | Unset = UNSET
    abstention_reason: EmbeddingChoiceAnswerAbstentionReason | Unset = UNSET

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

        choice: None | str
        choice = self.choice

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
                "choice": choice,
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

        d = dict(src_dict)
        name = d.pop("name")

        type_ = EmbeddingChoiceAnswerType(d.pop("type"))

        decision_method = EmbeddingChoiceAnswerDecisionMethod(d.pop("decision_method"))

        similarity_metric = EmbeddingChoiceAnswerSimilarityMetric(d.pop("similarity_metric"))

        similarities = []
        _similarities = d.pop("similarities")
        for similarities_item_data in _similarities:
            similarities_item = DecisionSimilarity.from_dict(similarities_item_data)

            similarities.append(similarities_item)

        margin = d.pop("margin")

        status = EmbeddingChoiceAnswerStatus(d.pop("status"))

        prototype_set_hash = d.pop("prototype_set_hash")

        def _parse_choice(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        choice = _parse_choice(d.pop("choice"))

        calibration_id = d.pop("calibration_id", UNSET)

        _abstention_reason = d.pop("abstention_reason", UNSET)
        abstention_reason: EmbeddingChoiceAnswerAbstentionReason | Unset
        if isinstance(_abstention_reason, Unset):
            abstention_reason = UNSET
        else:
            abstention_reason = EmbeddingChoiceAnswerAbstentionReason(_abstention_reason)

        embedding_choice_answer = cls(
            name=name,
            type_=type_,
            decision_method=decision_method,
            similarity_metric=similarity_metric,
            similarities=similarities,
            margin=margin,
            status=status,
            prototype_set_hash=prototype_set_hash,
            choice=choice,
            calibration_id=calibration_id,
            abstention_reason=abstention_reason,
        )

        return embedding_choice_answer
