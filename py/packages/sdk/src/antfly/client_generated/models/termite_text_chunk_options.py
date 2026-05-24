from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TermiteTextChunkOptions")


@_attrs_define
class TermiteTextChunkOptions:
    r"""Options specific to text chunking.

    Attributes:
        target_tokens (int | Unset): Target number of tokens per chunk.
        overlap_tokens (int | Unset): Number of tokens to overlap between consecutive chunks. Helps maintain context
            across chunk boundaries. Only used by fixed-size chunkers.
        separator (str | Unset): Separator string for splitting (e.g., '\n\n' for paragraphs). Only used by fixed-size
            chunkers.
    """

    target_tokens: int | Unset = UNSET
    overlap_tokens: int | Unset = UNSET
    separator: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        target_tokens = self.target_tokens

        overlap_tokens = self.overlap_tokens

        separator = self.separator

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if target_tokens is not UNSET:
            field_dict["target_tokens"] = target_tokens
        if overlap_tokens is not UNSET:
            field_dict["overlap_tokens"] = overlap_tokens
        if separator is not UNSET:
            field_dict["separator"] = separator

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        target_tokens = d.pop("target_tokens", UNSET)

        overlap_tokens = d.pop("overlap_tokens", UNSET)

        separator = d.pop("separator", UNSET)

        termite_text_chunk_options = cls(
            target_tokens=target_tokens,
            overlap_tokens=overlap_tokens,
            separator=separator,
        )

        termite_text_chunk_options.additional_properties = d
        return termite_text_chunk_options

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
