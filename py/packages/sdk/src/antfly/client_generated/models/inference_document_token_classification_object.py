from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_document_token_classification_object_object import (
    InferenceDocumentTokenClassificationObjectObject,
)

if TYPE_CHECKING:
    from ..models.inference_document_token_classification_prediction import (
        InferenceDocumentTokenClassificationPrediction,
    )


T = TypeVar("T", bound="InferenceDocumentTokenClassificationObject")


@_attrs_define
class InferenceDocumentTokenClassificationObject:
    """
    Attributes:
        object_ (InferenceDocumentTokenClassificationObjectObject):
        index (int):
        checkpoint_path (str):
        prefix (str):
        num_tokens (int):
        predictions (list[InferenceDocumentTokenClassificationPrediction]): Each result is an array of ClassifyResult
            sorted by score descending.
    """

    object_: InferenceDocumentTokenClassificationObjectObject
    index: int
    checkpoint_path: str
    prefix: str
    num_tokens: int
    predictions: list[InferenceDocumentTokenClassificationPrediction]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        index = self.index

        checkpoint_path = self.checkpoint_path

        prefix = self.prefix

        num_tokens = self.num_tokens

        predictions = []
        for predictions_item_data in self.predictions:
            predictions_item = predictions_item_data.to_dict()
            predictions.append(predictions_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "index": index,
                "checkpoint_path": checkpoint_path,
                "prefix": prefix,
                "num_tokens": num_tokens,
                "predictions": predictions,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_document_token_classification_prediction import (
            InferenceDocumentTokenClassificationPrediction,
        )

        d = dict(src_dict)
        object_ = InferenceDocumentTokenClassificationObjectObject(d.pop("object"))

        index = d.pop("index")

        checkpoint_path = d.pop("checkpoint_path")

        prefix = d.pop("prefix")

        num_tokens = d.pop("num_tokens")

        predictions = []
        _predictions = d.pop("predictions")
        for predictions_item_data in _predictions:
            predictions_item = InferenceDocumentTokenClassificationPrediction.from_dict(predictions_item_data)

            predictions.append(predictions_item)

        inference_document_token_classification_object = cls(
            object_=object_,
            index=index,
            checkpoint_path=checkpoint_path,
            prefix=prefix,
            num_tokens=num_tokens,
            predictions=predictions,
        )

        inference_document_token_classification_object.additional_properties = d
        return inference_document_token_classification_object

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
