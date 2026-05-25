from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_document_token_box import TermiteDocumentTokenBox


T = TypeVar("T", bound="TermiteDocumentTokenClassificationRequest")


@_attrs_define
class TermiteDocumentTokenClassificationRequest:
    """
    Attributes:
        model (str): Name or path of the document token classification model directory or checkpoint Example:
            acme/layoutdoc-token-tags.
        labels (list[str]): Labels in the same order expected by the checkpoint output head Example: ['O', 'B-KEY',
            'I-KEY'].
        tokens (list[TermiteDocumentTokenBox]):
        prefix (str | Unset): Optional tensor prefix inside the safetensors checkpoint Default: 'layoutdoc_token_head'.
            Example: layoutdoc_token_head.
    """

    model: str
    labels: list[str]
    tokens: list[TermiteDocumentTokenBox]
    prefix: str | Unset = "layoutdoc_token_head"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        labels = self.labels

        tokens = []
        for tokens_item_data in self.tokens:
            tokens_item = tokens_item_data.to_dict()
            tokens.append(tokens_item)

        prefix = self.prefix

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "labels": labels,
                "tokens": tokens,
            }
        )
        if prefix is not UNSET:
            field_dict["prefix"] = prefix

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_document_token_box import TermiteDocumentTokenBox

        d = dict(src_dict)
        model = d.pop("model")

        labels = cast(list[str], d.pop("labels"))

        tokens = []
        _tokens = d.pop("tokens")
        for tokens_item_data in _tokens:
            tokens_item = TermiteDocumentTokenBox.from_dict(tokens_item_data)

            tokens.append(tokens_item)

        prefix = d.pop("prefix", UNSET)

        termite_document_token_classification_request = cls(
            model=model,
            labels=labels,
            tokens=tokens,
            prefix=prefix,
        )

        termite_document_token_classification_request.additional_properties = d
        return termite_document_token_classification_request

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
