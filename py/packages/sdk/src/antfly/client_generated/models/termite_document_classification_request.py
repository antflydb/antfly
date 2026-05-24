from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TermiteDocumentClassificationRequest")


@_attrs_define
class TermiteDocumentClassificationRequest:
    """
    Attributes:
        model (str): Name or path of the document classification model directory or checkpoint Example: acme/layoutdoc-
            invoice-sequence.
        image_path (str): Absolute or server-local path to the page image Example: /tmp/page.png.
        num_tokens (int): Number of OCR/text tokens associated with the page Example: 42.
        labels (list[str]): Labels in the same order expected by the checkpoint output head Example: ['invoice', 'form',
            'email'].
        prefix (str | Unset): Optional tensor prefix inside the safetensors checkpoint Default:
            'layoutdoc_sequence_head'. Example: layoutdoc_sequence_head.
    """

    model: str
    image_path: str
    num_tokens: int
    labels: list[str]
    prefix: str | Unset = "layoutdoc_sequence_head"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        image_path = self.image_path

        num_tokens = self.num_tokens

        labels = self.labels

        prefix = self.prefix

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "image_path": image_path,
                "num_tokens": num_tokens,
                "labels": labels,
            }
        )
        if prefix is not UNSET:
            field_dict["prefix"] = prefix

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        image_path = d.pop("image_path")

        num_tokens = d.pop("num_tokens")

        labels = cast(list[str], d.pop("labels"))

        prefix = d.pop("prefix", UNSET)

        termite_document_classification_request = cls(
            model=model,
            image_path=image_path,
            num_tokens=num_tokens,
            labels=labels,
            prefix=prefix,
        )

        termite_document_classification_request.additional_properties = d
        return termite_document_classification_request

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
