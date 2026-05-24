from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TermiteClassifyRequest")


@_attrs_define
class TermiteClassifyRequest:
    """
    Attributes:
        model (str): Name of classifier model from models_dir/classifiers/ Example: MoritzLaurer/mDeBERTa-v3-base-mnli-
            xnli.
        texts (list[str]): Texts to classify Example: ['I love this product!', 'The service was terrible.'].
        labels (list[str]): Candidate labels for zero-shot classification.
            The model will predict which label(s) best describe each text.
             Example: ['positive', 'negative', 'neutral'].
        hypothesis_template (str | Unset): Custom hypothesis template for NLI-based classification.
            Use "{}" as placeholder for the label.
            Default: "This example is {}."
             Example: This text expresses a {} sentiment..
        multi_label (bool | Unset): If true, allows multiple labels per text (independent scoring).
            If false (default), scores are normalized across labels.
             Default: False.
    """

    model: str
    texts: list[str]
    labels: list[str]
    hypothesis_template: str | Unset = UNSET
    multi_label: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        texts = self.texts

        labels = self.labels

        hypothesis_template = self.hypothesis_template

        multi_label = self.multi_label

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "texts": texts,
                "labels": labels,
            }
        )
        if hypothesis_template is not UNSET:
            field_dict["hypothesis_template"] = hypothesis_template
        if multi_label is not UNSET:
            field_dict["multi_label"] = multi_label

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        texts = cast(list[str], d.pop("texts"))

        labels = cast(list[str], d.pop("labels"))

        hypothesis_template = d.pop("hypothesis_template", UNSET)

        multi_label = d.pop("multi_label", UNSET)

        termite_classify_request = cls(
            model=model,
            texts=texts,
            labels=labels,
            hypothesis_template=hypothesis_template,
            multi_label=multi_label,
        )

        termite_classify_request.additional_properties = d
        return termite_classify_request

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
