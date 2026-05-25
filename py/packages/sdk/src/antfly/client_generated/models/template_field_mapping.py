from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.antfly_type_2 import AntflyType2
from ..types import UNSET, Unset

T = TypeVar("T", bound="TemplateFieldMapping")


@_attrs_define
class TemplateFieldMapping:
    """Field mapping to apply when a dynamic template matches

    Attributes:
        type_ (AntflyType2 | Unset): Field type annotations for schema fields
        analyzer (str | Unset): Analyzer name (e.g., "standard", "keyword", "en", "html_analyzer").
            Used for text fields to control tokenization and normalization.
        index (bool | Unset): Whether to index the field (default true) Default: True.
        store (bool | Unset): Whether to store the field value (default false) Default: False.
        include_in_all (bool | Unset): Whether to include in the _all field for cross-field search Default: False.
        doc_values (bool | Unset): Whether to enable doc values for sorting/faceting Default: False.
    """

    type_: AntflyType2 | Unset = UNSET
    analyzer: str | Unset = UNSET
    index: bool | Unset = True
    store: bool | Unset = False
    include_in_all: bool | Unset = False
    doc_values: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        analyzer = self.analyzer

        index = self.index

        store = self.store

        include_in_all = self.include_in_all

        doc_values = self.doc_values

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if type_ is not UNSET:
            field_dict["type"] = type_
        if analyzer is not UNSET:
            field_dict["analyzer"] = analyzer
        if index is not UNSET:
            field_dict["index"] = index
        if store is not UNSET:
            field_dict["store"] = store
        if include_in_all is not UNSET:
            field_dict["include_in_all"] = include_in_all
        if doc_values is not UNSET:
            field_dict["doc_values"] = doc_values

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _type_ = d.pop("type", UNSET)
        type_: AntflyType2 | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = AntflyType2(_type_)

        analyzer = d.pop("analyzer", UNSET)

        index = d.pop("index", UNSET)

        store = d.pop("store", UNSET)

        include_in_all = d.pop("include_in_all", UNSET)

        doc_values = d.pop("doc_values", UNSET)

        template_field_mapping = cls(
            type_=type_,
            analyzer=analyzer,
            index=index,
            store=store,
            include_in_all=include_in_all,
            doc_values=doc_values,
        )

        template_field_mapping.additional_properties = d
        return template_field_mapping

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
