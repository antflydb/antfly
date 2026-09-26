from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="QueryHighlight")


@_attrs_define
class QueryHighlight:
    """Ask for highlighted fragments of the stored fields matched by
    `full_text_search` and by named full-text queries. Matches are located
    by re-analyzing the stored value with the field's analyzer, so stemmed
    and stop-word-filtered terms highlight the surface form. `prefix`,
    `wildcard`, `regexp`, and `fuzzy` clauses mark whole tokens; `match`,
    `match_phrase`, or `prefix` on a `substring` companion
    (`field._substring`) marks the exact contained bytes, including
    matches that span two adjacent words.

        Attributes:
            fields (list[str] | Unset): Source fields to highlight. Defaults to every field the full-text
                query references (companion suffixes such as `._substring` and
                `.keyword` resolve to their root field).
                 Example: ['title', 'body'].
            fragment_size (int | Unset): Fragment window size in bytes. Default: 150.
            max_fragments (int | Unset): Maximum fragments returned per field. Default: 3.
    """

    fields: list[str] | Unset = UNSET
    fragment_size: int | Unset = 150
    max_fragments: int | Unset = 3
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        fragment_size = self.fragment_size

        max_fragments = self.max_fragments

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if fields is not UNSET:
            field_dict["fields"] = fields
        if fragment_size is not UNSET:
            field_dict["fragment_size"] = fragment_size
        if max_fragments is not UNSET:
            field_dict["max_fragments"] = max_fragments

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        fields = cast(list[str], d.pop("fields", UNSET))

        fragment_size = d.pop("fragment_size", UNSET)

        max_fragments = d.pop("max_fragments", UNSET)

        query_highlight = cls(
            fields=fields,
            fragment_size=fragment_size,
            max_fragments=max_fragments,
        )

        query_highlight.additional_properties = d
        return query_highlight

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
