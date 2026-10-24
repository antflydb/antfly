from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.vector_labels_config_labels_item import VectorLabelsConfigLabelsItem


T = TypeVar("T", bound="VectorLabelsConfig")


@_attrs_define
class VectorLabelsConfig:
    """Zero-shot label stage over accepted dense embedding artifacts. Label prompts use the source embedding producer;
    changing this vocabulary rebuilds labels without reading or decoding media. Scores are raw dot products, not
    probabilities. The asset enrichment must name a dense embedding enrichment in source_artifact_name.

        Attributes:
            labels (list[VectorLabelsConfigLabelsItem]):
            top_k (int | Unset):  Default: 3.
            min_score (float | Unset):
    """

    labels: list[VectorLabelsConfigLabelsItem]
    top_k: int | Unset = 3
    min_score: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        labels = []
        for labels_item_data in self.labels:
            labels_item = labels_item_data.to_dict()
            labels.append(labels_item)

        top_k = self.top_k

        min_score = self.min_score

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "labels": labels,
            }
        )
        if top_k is not UNSET:
            field_dict["top_k"] = top_k
        if min_score is not UNSET:
            field_dict["min_score"] = min_score

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.vector_labels_config_labels_item import VectorLabelsConfigLabelsItem

        d = dict(src_dict)
        labels = []
        _labels = d.pop("labels")
        for labels_item_data in _labels:
            labels_item = VectorLabelsConfigLabelsItem.from_dict(labels_item_data)

            labels.append(labels_item)

        top_k = d.pop("top_k", UNSET)

        min_score = d.pop("min_score", UNSET)

        vector_labels_config = cls(
            labels=labels,
            top_k=top_k,
            min_score=min_score,
        )

        return vector_labels_config
