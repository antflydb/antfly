from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_document_token_classification_features import TermiteDocumentTokenClassificationFeatures
    from ..models.termite_document_token_classification_result import TermiteDocumentTokenClassificationResult


T = TypeVar("T", bound="TermiteDocumentTokenClassificationPrediction")


@_attrs_define
class TermiteDocumentTokenClassificationPrediction:
    """
    Attributes:
        token_index (int):
        text (str):
        bbox (list[int]):
        features (TermiteDocumentTokenClassificationFeatures):
        scores (list[TermiteDocumentTokenClassificationResult]):
        best (None | TermiteDocumentTokenClassificationResult | Unset):
    """

    token_index: int
    text: str
    bbox: list[int]
    features: TermiteDocumentTokenClassificationFeatures
    scores: list[TermiteDocumentTokenClassificationResult]
    best: None | TermiteDocumentTokenClassificationResult | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.termite_document_token_classification_result import TermiteDocumentTokenClassificationResult

        token_index = self.token_index

        text = self.text

        bbox = self.bbox

        features = self.features.to_dict()

        scores = []
        for scores_item_data in self.scores:
            scores_item = scores_item_data.to_dict()
            scores.append(scores_item)

        best: dict[str, Any] | None | Unset
        if isinstance(self.best, Unset):
            best = UNSET
        elif isinstance(self.best, TermiteDocumentTokenClassificationResult):
            best = self.best.to_dict()
        else:
            best = self.best

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "token_index": token_index,
                "text": text,
                "bbox": bbox,
                "features": features,
                "scores": scores,
            }
        )
        if best is not UNSET:
            field_dict["best"] = best

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_document_token_classification_features import TermiteDocumentTokenClassificationFeatures
        from ..models.termite_document_token_classification_result import TermiteDocumentTokenClassificationResult

        d = dict(src_dict)
        token_index = d.pop("token_index")

        text = d.pop("text")

        bbox = cast(list[int], d.pop("bbox"))

        features = TermiteDocumentTokenClassificationFeatures.from_dict(d.pop("features"))

        scores = []
        _scores = d.pop("scores")
        for scores_item_data in _scores:
            scores_item = TermiteDocumentTokenClassificationResult.from_dict(scores_item_data)

            scores.append(scores_item)

        def _parse_best(data: object) -> None | TermiteDocumentTokenClassificationResult | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                best_type_1 = TermiteDocumentTokenClassificationResult.from_dict(data)

                return best_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | TermiteDocumentTokenClassificationResult | Unset, data)

        best = _parse_best(d.pop("best", UNSET))

        termite_document_token_classification_prediction = cls(
            token_index=token_index,
            text=text,
            bbox=bbox,
            features=features,
            scores=scores,
            best=best,
        )

        termite_document_token_classification_prediction.additional_properties = d
        return termite_document_token_classification_prediction

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
