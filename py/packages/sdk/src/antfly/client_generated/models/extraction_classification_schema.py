from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionClassificationSchema")


@_attrs_define
class ExtractionClassificationSchema:
    """
    Attributes:
        name (str):
        labels (list[str]):
        multi_label (bool | Unset): When false, return the highest-ranked labels up to `top_k` (one by
            default). When true, return every label meeting `options.threshold`.
             Default: False.
        hypothesis_template (str | Unset): NLI hypothesis template for this named taxonomy. Use `{}` as the
            candidate-label placeholder. Non-NLI extractors ignore this field.
             Default: 'This example is {}.'.
        top_k (int | Unset): Maximum labels returned for single-label classification. Ignored
            when `multi_label` is true, where `options.threshold` controls the
            returned set.
             Default: 1.
    """

    name: str
    labels: list[str]
    multi_label: bool | Unset = False
    hypothesis_template: str | Unset = "This example is {}."
    top_k: int | Unset = 1
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        labels = self.labels

        multi_label = self.multi_label

        hypothesis_template = self.hypothesis_template

        top_k = self.top_k

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "labels": labels,
            }
        )
        if multi_label is not UNSET:
            field_dict["multi_label"] = multi_label
        if hypothesis_template is not UNSET:
            field_dict["hypothesis_template"] = hypothesis_template
        if top_k is not UNSET:
            field_dict["top_k"] = top_k

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        labels = cast(list[str], d.pop("labels"))

        multi_label = d.pop("multi_label", UNSET)

        hypothesis_template = d.pop("hypothesis_template", UNSET)

        top_k = d.pop("top_k", UNSET)

        extraction_classification_schema = cls(
            name=name,
            labels=labels,
            multi_label=multi_label,
            hypothesis_template=hypothesis_template,
            top_k=top_k,
        )

        extraction_classification_schema.additional_properties = d
        return extraction_classification_schema

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
