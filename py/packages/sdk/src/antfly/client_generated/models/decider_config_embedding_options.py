from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.decider_config_embedding_options_task_type import DeciderConfigEmbeddingOptionsTaskType
from ..types import UNSET, Unset

T = TypeVar("T", bound="DeciderConfigEmbeddingOptions")


@_attrs_define
class DeciderConfigEmbeddingOptions:
    """Geometry and acceptance defaults for embedding_similarity decisions. Named embedders require dimensions matching
    their configured dense output. Direct Antfly model routes support EmbeddingGemma 2 dimensions and fitted
    calibrations. Acceptance is serialized onto each question and can be overridden per question. Raw cosine
    similarities never represent probabilities.

        Attributes:
            calibration_id (str | Unset): Qualified fitted raw thresholds in model/calibrations/{id}.json. Bound to the
                exact asset identity, task, renderer, dimensions and category prototypes; mutually exclusive with manual
                thresholds. Does not produce probabilities.
            task_type (DeciderConfigEmbeddingOptionsTaskType | Unset):  Default:
                DeciderConfigEmbeddingOptionsTaskType.CLUSTERING.
            dimensions (int | Unset): Must match the named embedder's configured dimensions. Without embedder, the direct
                EmbeddingGemma 2 route accepts only 768, 512, 256 or 128. Default: 768.
            min_similarity (float | Unset):
            min_margin (float | Unset):
    """

    calibration_id: str | Unset = UNSET
    task_type: DeciderConfigEmbeddingOptionsTaskType | Unset = DeciderConfigEmbeddingOptionsTaskType.CLUSTERING
    dimensions: int | Unset = 768
    min_similarity: float | Unset = UNSET
    min_margin: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        calibration_id = self.calibration_id

        task_type: str | Unset = UNSET
        if not isinstance(self.task_type, Unset):
            task_type = self.task_type.value

        dimensions = self.dimensions

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
        task_type: DeciderConfigEmbeddingOptionsTaskType | Unset
        if isinstance(_task_type, Unset):
            task_type = UNSET
        else:
            task_type = DeciderConfigEmbeddingOptionsTaskType(_task_type)

        dimensions = d.pop("dimensions", UNSET)

        min_similarity = d.pop("min_similarity", UNSET)

        min_margin = d.pop("min_margin", UNSET)

        decider_config_embedding_options = cls(
            calibration_id=calibration_id,
            task_type=task_type,
            dimensions=dimensions,
            min_similarity=min_similarity,
            min_margin=min_margin,
        )

        return decider_config_embedding_options
