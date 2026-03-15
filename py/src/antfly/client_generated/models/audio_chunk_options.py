from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AudioChunkOptions")


@_attrs_define
class AudioChunkOptions:
    """Options specific to audio chunking.

    Attributes:
        window_duration_ms (Union[Unset, int]): Window duration in milliseconds for fixed-window audio chunking
            (default: 30000).
        overlap_duration_ms (Union[Unset, int]): Overlap duration in milliseconds between audio chunks (default: 0).
    """

    window_duration_ms: Union[Unset, int] = UNSET
    overlap_duration_ms: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        window_duration_ms = self.window_duration_ms

        overlap_duration_ms = self.overlap_duration_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if window_duration_ms is not UNSET:
            field_dict["window_duration_ms"] = window_duration_ms
        if overlap_duration_ms is not UNSET:
            field_dict["overlap_duration_ms"] = overlap_duration_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        window_duration_ms = d.pop("window_duration_ms", UNSET)

        overlap_duration_ms = d.pop("overlap_duration_ms", UNSET)

        audio_chunk_options = cls(
            window_duration_ms=window_duration_ms,
            overlap_duration_ms=overlap_duration_ms,
        )

        audio_chunk_options.additional_properties = d
        return audio_chunk_options

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
