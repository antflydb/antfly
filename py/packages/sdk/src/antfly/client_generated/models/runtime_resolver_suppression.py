from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.curation_suppression_entry import CurationSuppressionEntry


T = TypeVar("T", bound="RuntimeResolverSuppression")


@_attrs_define
class RuntimeResolverSuppression:
    """
    Attributes:
        keys (list[str] | Unset):
        texts (list[str] | Unset):
        min_text_chars (int | Unset):
        entries (list[CurationSuppressionEntry] | Unset):
        text_entries (list[CurationSuppressionEntry] | Unset):
    """

    keys: list[str] | Unset = UNSET
    texts: list[str] | Unset = UNSET
    min_text_chars: int | Unset = UNSET
    entries: list[CurationSuppressionEntry] | Unset = UNSET
    text_entries: list[CurationSuppressionEntry] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        keys: list[str] | Unset = UNSET
        if not isinstance(self.keys, Unset):
            keys = self.keys

        texts: list[str] | Unset = UNSET
        if not isinstance(self.texts, Unset):
            texts = self.texts

        min_text_chars = self.min_text_chars

        entries: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.entries, Unset):
            entries = []
            for entries_item_data in self.entries:
                entries_item = entries_item_data.to_dict()
                entries.append(entries_item)

        text_entries: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.text_entries, Unset):
            text_entries = []
            for text_entries_item_data in self.text_entries:
                text_entries_item = text_entries_item_data.to_dict()
                text_entries.append(text_entries_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if keys is not UNSET:
            field_dict["keys"] = keys
        if texts is not UNSET:
            field_dict["texts"] = texts
        if min_text_chars is not UNSET:
            field_dict["min_text_chars"] = min_text_chars
        if entries is not UNSET:
            field_dict["entries"] = entries
        if text_entries is not UNSET:
            field_dict["text_entries"] = text_entries

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.curation_suppression_entry import CurationSuppressionEntry

        d = dict(src_dict)
        keys = cast(list[str], d.pop("keys", UNSET))

        texts = cast(list[str], d.pop("texts", UNSET))

        min_text_chars = d.pop("min_text_chars", UNSET)

        _entries = d.pop("entries", UNSET)
        entries: list[CurationSuppressionEntry] | Unset = UNSET
        if _entries is not UNSET:
            entries = []
            for entries_item_data in _entries:
                entries_item = CurationSuppressionEntry.from_dict(entries_item_data)

                entries.append(entries_item)

        _text_entries = d.pop("text_entries", UNSET)
        text_entries: list[CurationSuppressionEntry] | Unset = UNSET
        if _text_entries is not UNSET:
            text_entries = []
            for text_entries_item_data in _text_entries:
                text_entries_item = CurationSuppressionEntry.from_dict(text_entries_item_data)

                text_entries.append(text_entries_item)

        runtime_resolver_suppression = cls(
            keys=keys,
            texts=texts,
            min_text_chars=min_text_chars,
            entries=entries,
            text_entries=text_entries,
        )

        runtime_resolver_suppression.additional_properties = d
        return runtime_resolver_suppression

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
