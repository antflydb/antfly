from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_document_classification_object_object import InferenceDocumentClassificationObjectObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_document_classification_features import InferenceDocumentClassificationFeatures
    from ..models.inference_document_classification_object_input import InferenceDocumentClassificationObjectInput
    from ..models.inference_document_classification_result import InferenceDocumentClassificationResult


T = TypeVar("T", bound="InferenceDocumentClassificationObject")


@_attrs_define
class InferenceDocumentClassificationObject:
    """
    Attributes:
        object_ (InferenceDocumentClassificationObjectObject):
        index (int):
        checkpoint_path (str):
        prefix (str):
        input_ (InferenceDocumentClassificationObjectInput):
        features (InferenceDocumentClassificationFeatures):
        scores (list[InferenceDocumentClassificationResult]):
        best (InferenceDocumentClassificationResult | None | Unset):
    """

    object_: InferenceDocumentClassificationObjectObject
    index: int
    checkpoint_path: str
    prefix: str
    input_: InferenceDocumentClassificationObjectInput
    features: InferenceDocumentClassificationFeatures
    scores: list[InferenceDocumentClassificationResult]
    best: InferenceDocumentClassificationResult | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.inference_document_classification_result import InferenceDocumentClassificationResult

        object_ = self.object_.value

        index = self.index

        checkpoint_path = self.checkpoint_path

        prefix = self.prefix

        input_ = self.input_.to_dict()

        features = self.features.to_dict()

        scores = []
        for scores_item_data in self.scores:
            scores_item = scores_item_data.to_dict()
            scores.append(scores_item)

        best: dict[str, Any] | None | Unset
        if isinstance(self.best, Unset):
            best = UNSET
        elif isinstance(self.best, InferenceDocumentClassificationResult):
            best = self.best.to_dict()
        else:
            best = self.best

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "index": index,
                "checkpoint_path": checkpoint_path,
                "prefix": prefix,
                "input": input_,
                "features": features,
                "scores": scores,
            }
        )
        if best is not UNSET:
            field_dict["best"] = best

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_document_classification_features import InferenceDocumentClassificationFeatures
        from ..models.inference_document_classification_object_input import InferenceDocumentClassificationObjectInput
        from ..models.inference_document_classification_result import InferenceDocumentClassificationResult

        d = dict(src_dict)
        object_ = InferenceDocumentClassificationObjectObject(d.pop("object"))

        index = d.pop("index")

        checkpoint_path = d.pop("checkpoint_path")

        prefix = d.pop("prefix")

        input_ = InferenceDocumentClassificationObjectInput.from_dict(d.pop("input"))

        features = InferenceDocumentClassificationFeatures.from_dict(d.pop("features"))

        scores = []
        _scores = d.pop("scores")
        for scores_item_data in _scores:
            scores_item = InferenceDocumentClassificationResult.from_dict(scores_item_data)

            scores.append(scores_item)

        def _parse_best(data: object) -> InferenceDocumentClassificationResult | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                best_type_1 = InferenceDocumentClassificationResult.from_dict(data)

                return best_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(InferenceDocumentClassificationResult | None | Unset, data)

        best = _parse_best(d.pop("best", UNSET))

        inference_document_classification_object = cls(
            object_=object_,
            index=index,
            checkpoint_path=checkpoint_path,
            prefix=prefix,
            input_=input_,
            features=features,
            scores=scores,
            best=best,
        )

        inference_document_classification_object.additional_properties = d
        return inference_document_classification_object

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
