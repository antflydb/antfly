from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.embedding_decision_options_dimensions import EmbeddingDecisionOptionsDimensions
from ..models.embedding_decision_options_task_type import EmbeddingDecisionOptionsTaskType
from ..types import UNSET, Unset

T = TypeVar("T", bound="EmbeddingDecisionOptions")


@_attrs_define
class EmbeddingDecisionOptions:
    """Request-wide embedding geometry defaults. Acceptance thresholds and calibration belong to individual questions.

    Attributes:
        task_type (EmbeddingDecisionOptionsTaskType | Unset): Prompt profile for the decision input and category
            prototypes. The embedding endpoint also accepts task_type; decision routing restricts it to these two profiles.
            Default: EmbeddingDecisionOptionsTaskType.CLUSTERING.
        dimensions (EmbeddingDecisionOptionsDimensions | Unset): Truncate and renormalize embeddings to this dimension
            before cosine scoring. Default: EmbeddingDecisionOptionsDimensions.VALUE_768.
    """

    task_type: EmbeddingDecisionOptionsTaskType | Unset = EmbeddingDecisionOptionsTaskType.CLUSTERING
    dimensions: EmbeddingDecisionOptionsDimensions | Unset = EmbeddingDecisionOptionsDimensions.VALUE_768

    def to_dict(self) -> dict[str, Any]:
        task_type: str | Unset = UNSET
        if not isinstance(self.task_type, Unset):
            task_type = self.task_type.value

        dimensions: int | Unset = UNSET
        if not isinstance(self.dimensions, Unset):
            dimensions = self.dimensions.value

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if task_type is not UNSET:
            field_dict["task_type"] = task_type
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _task_type = d.pop("task_type", UNSET)
        task_type: EmbeddingDecisionOptionsTaskType | Unset
        if isinstance(_task_type, Unset):
            task_type = UNSET
        else:
            task_type = EmbeddingDecisionOptionsTaskType(_task_type)

        _dimensions = d.pop("dimensions", UNSET)
        dimensions: EmbeddingDecisionOptionsDimensions | Unset
        if isinstance(_dimensions, Unset):
            dimensions = UNSET
        else:
            dimensions = EmbeddingDecisionOptionsDimensions(_dimensions)

        embedding_decision_options = cls(
            task_type=task_type,
            dimensions=dimensions,
        )

        return embedding_decision_options
