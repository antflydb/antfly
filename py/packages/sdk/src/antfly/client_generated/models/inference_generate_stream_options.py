from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceGenerateStreamOptions")


@_attrs_define
class InferenceGenerateStreamOptions:
    """Options that apply only to streamed generation responses.

    Attributes:
        include_usage (bool | Unset): When true, emit a final usage-only chunk before the `[DONE]` sentinel.
            All preceding chunks carry null or omitted usage.
             Default: False.
    """

    include_usage: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        include_usage = self.include_usage

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if include_usage is not UNSET:
            field_dict["include_usage"] = include_usage

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        include_usage = d.pop("include_usage", UNSET)

        inference_generate_stream_options = cls(
            include_usage=include_usage,
        )

        inference_generate_stream_options.additional_properties = d
        return inference_generate_stream_options

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
