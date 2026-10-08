from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.embedding_extraction_decision_decision_method import EmbeddingExtractionDecisionDecisionMethod
from ..models.embedding_extraction_decision_mode import EmbeddingExtractionDecisionMode
from ..models.embedding_extraction_decision_status import EmbeddingExtractionDecisionStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.embedding_extraction_decision_similarities import EmbeddingExtractionDecisionSimilarities


T = TypeVar("T", bound="EmbeddingExtractionDecision")


@_attrs_define
class EmbeddingExtractionDecision:
    """Embedding classification results report raw cosine similarities and no confidence or probabilities.

    Attributes:
        name (str):
        mode (EmbeddingExtractionDecisionMode):
        decision_method (EmbeddingExtractionDecisionDecisionMethod):
        labels (list[str]):
        similarities (EmbeddingExtractionDecisionSimilarities):
        status (EmbeddingExtractionDecisionStatus):
        prototype_set_hash (str | Unset):
        calibration_id (str | Unset):
        margin (float | Unset):
        abstention_reason (str | Unset):
    """

    name: str
    mode: EmbeddingExtractionDecisionMode
    decision_method: EmbeddingExtractionDecisionDecisionMethod
    labels: list[str]
    similarities: EmbeddingExtractionDecisionSimilarities
    status: EmbeddingExtractionDecisionStatus
    prototype_set_hash: str | Unset = UNSET
    calibration_id: str | Unset = UNSET
    margin: float | Unset = UNSET
    abstention_reason: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        mode = self.mode.value

        decision_method = self.decision_method.value

        labels = self.labels

        similarities = self.similarities.to_dict()

        status = self.status.value

        prototype_set_hash = self.prototype_set_hash

        calibration_id = self.calibration_id

        margin = self.margin

        abstention_reason = self.abstention_reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "mode": mode,
                "decision_method": decision_method,
                "labels": labels,
                "similarities": similarities,
                "status": status,
            }
        )
        if prototype_set_hash is not UNSET:
            field_dict["prototype_set_hash"] = prototype_set_hash
        if calibration_id is not UNSET:
            field_dict["calibration_id"] = calibration_id
        if margin is not UNSET:
            field_dict["margin"] = margin
        if abstention_reason is not UNSET:
            field_dict["abstention_reason"] = abstention_reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.embedding_extraction_decision_similarities import EmbeddingExtractionDecisionSimilarities

        d = dict(src_dict)
        name = d.pop("name")

        mode = EmbeddingExtractionDecisionMode(d.pop("mode"))

        decision_method = EmbeddingExtractionDecisionDecisionMethod(d.pop("decision_method"))

        labels = cast(list[str], d.pop("labels"))

        similarities = EmbeddingExtractionDecisionSimilarities.from_dict(d.pop("similarities"))

        status = EmbeddingExtractionDecisionStatus(d.pop("status"))

        prototype_set_hash = d.pop("prototype_set_hash", UNSET)

        calibration_id = d.pop("calibration_id", UNSET)

        margin = d.pop("margin", UNSET)

        abstention_reason = d.pop("abstention_reason", UNSET)

        embedding_extraction_decision = cls(
            name=name,
            mode=mode,
            decision_method=decision_method,
            labels=labels,
            similarities=similarities,
            status=status,
            prototype_set_hash=prototype_set_hash,
            calibration_id=calibration_id,
            margin=margin,
            abstention_reason=abstention_reason,
        )

        embedding_extraction_decision.additional_properties = d
        return embedding_extraction_decision

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
