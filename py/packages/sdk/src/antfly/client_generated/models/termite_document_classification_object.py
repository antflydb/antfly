from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_document_classification_object_object import TermiteDocumentClassificationObjectObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_document_classification_features import TermiteDocumentClassificationFeatures
    from ..models.termite_document_classification_object_input import TermiteDocumentClassificationObjectInput
    from ..models.termite_document_classification_result import TermiteDocumentClassificationResult


T = TypeVar("T", bound="TermiteDocumentClassificationObject")


@_attrs_define
class TermiteDocumentClassificationObject:
    """
    Attributes:
        object_ (TermiteDocumentClassificationObjectObject):
        index (int):
        checkpoint_path (str):
        prefix (str):
        input_ (TermiteDocumentClassificationObjectInput):
        features (TermiteDocumentClassificationFeatures):
        scores (list[TermiteDocumentClassificationResult]):
        best (None | TermiteDocumentClassificationResult | Unset):
    """

    object_: TermiteDocumentClassificationObjectObject
    index: int
    checkpoint_path: str
    prefix: str
    input_: TermiteDocumentClassificationObjectInput
    features: TermiteDocumentClassificationFeatures
    scores: list[TermiteDocumentClassificationResult]
    best: None | TermiteDocumentClassificationResult | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.termite_document_classification_result import TermiteDocumentClassificationResult

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
        elif isinstance(self.best, TermiteDocumentClassificationResult):
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
        from ..models.termite_document_classification_features import TermiteDocumentClassificationFeatures
        from ..models.termite_document_classification_object_input import TermiteDocumentClassificationObjectInput
        from ..models.termite_document_classification_result import TermiteDocumentClassificationResult

        d = dict(src_dict)
        object_ = TermiteDocumentClassificationObjectObject(d.pop("object"))

        index = d.pop("index")

        checkpoint_path = d.pop("checkpoint_path")

        prefix = d.pop("prefix")

        input_ = TermiteDocumentClassificationObjectInput.from_dict(d.pop("input"))

        features = TermiteDocumentClassificationFeatures.from_dict(d.pop("features"))

        scores = []
        _scores = d.pop("scores")
        for scores_item_data in _scores:
            scores_item = TermiteDocumentClassificationResult.from_dict(scores_item_data)

            scores.append(scores_item)

        def _parse_best(data: object) -> None | TermiteDocumentClassificationResult | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                best_type_1 = TermiteDocumentClassificationResult.from_dict(data)

                return best_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | TermiteDocumentClassificationResult | Unset, data)

        best = _parse_best(d.pop("best", UNSET))

        termite_document_classification_object = cls(
            object_=object_,
            index=index,
            checkpoint_path=checkpoint_path,
            prefix=prefix,
            input_=input_,
            features=features,
            scores=scores,
            best=best,
        )

        termite_document_classification_object.additional_properties = d
        return termite_document_classification_object

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
