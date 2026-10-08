from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_embedding_decision_options_dimensions import InferenceEmbeddingDecisionOptionsDimensions
from ..models.inference_embedding_decision_options_task_type import InferenceEmbeddingDecisionOptionsTaskType
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceEmbeddingDecisionOptions")


@_attrs_define
class InferenceEmbeddingDecisionOptions:
    """EmbeddingGemma 2 similarity decisions only. Other models reject these options. Uncalibrated results contain cosine
    scores and never probabilities.

        Attributes:
            calibration_id (str | Unset): Qualified fitted raw thresholds in model/calibrations/{id}.json. Bound to the
                exact asset identity, task, renderer, dimensions and category prototypes; mutually exclusive with manual
                thresholds. Does not produce probabilities.
            task_type (InferenceEmbeddingDecisionOptionsTaskType | Unset):  Default:
                InferenceEmbeddingDecisionOptionsTaskType.CLUSTERING.
            dimensions (InferenceEmbeddingDecisionOptionsDimensions | Unset):  Default:
                InferenceEmbeddingDecisionOptionsDimensions.VALUE_768.
            min_similarity (float | Unset):
            min_margin (float | Unset):
    """

    calibration_id: str | Unset = UNSET
    task_type: InferenceEmbeddingDecisionOptionsTaskType | Unset = InferenceEmbeddingDecisionOptionsTaskType.CLUSTERING
    dimensions: InferenceEmbeddingDecisionOptionsDimensions | Unset = (
        InferenceEmbeddingDecisionOptionsDimensions.VALUE_768
    )
    min_similarity: float | Unset = UNSET
    min_margin: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        calibration_id = self.calibration_id

        task_type: str | Unset = UNSET
        if not isinstance(self.task_type, Unset):
            task_type = self.task_type.value

        dimensions: int | Unset = UNSET
        if not isinstance(self.dimensions, Unset):
            dimensions = self.dimensions.value

        min_similarity = self.min_similarity

        min_margin = self.min_margin

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if calibration_id is not UNSET:
            field_dict["calibration_id"] = calibration_id
        if task_type is not UNSET:
            field_dict["task_type"] = task_type
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if min_similarity is not UNSET:
            field_dict["min_similarity"] = min_similarity
        if min_margin is not UNSET:
            field_dict["min_margin"] = min_margin

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        calibration_id = d.pop("calibration_id", UNSET)

        _task_type = d.pop("task_type", UNSET)
        task_type: InferenceEmbeddingDecisionOptionsTaskType | Unset
        if isinstance(_task_type, Unset):
            task_type = UNSET
        else:
            task_type = InferenceEmbeddingDecisionOptionsTaskType(_task_type)

        _dimensions = d.pop("dimensions", UNSET)
        dimensions: InferenceEmbeddingDecisionOptionsDimensions | Unset
        if isinstance(_dimensions, Unset):
            dimensions = UNSET
        else:
            dimensions = InferenceEmbeddingDecisionOptionsDimensions(_dimensions)

        min_similarity = d.pop("min_similarity", UNSET)

        min_margin = d.pop("min_margin", UNSET)

        inference_embedding_decision_options = cls(
            calibration_id=calibration_id,
            task_type=task_type,
            dimensions=dimensions,
            min_similarity=min_similarity,
            min_margin=min_margin,
        )

        return inference_embedding_decision_options
