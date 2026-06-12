from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceBinaryContent")


@_attrs_define
class InferenceBinaryContent:
    """Binary media content with format-specific metadata.

    Attributes:
        data (str | Unset): Base64-encoded binary data (valid WAV, PNG, etc.)
        start_time_ms (float | Unset): Audio: window start time in milliseconds
        end_time_ms (float | Unset): Audio: window end time in milliseconds
        frame_index (int | Unset): Animation: frame number
        frame_delay_ms (int | Unset): Animation: display delay in milliseconds
    """

    data: str | Unset = UNSET
    start_time_ms: float | Unset = UNSET
    end_time_ms: float | Unset = UNSET
    frame_index: int | Unset = UNSET
    frame_delay_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = self.data

        start_time_ms = self.start_time_ms

        end_time_ms = self.end_time_ms

        frame_index = self.frame_index

        frame_delay_ms = self.frame_delay_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if data is not UNSET:
            field_dict["data"] = data
        if start_time_ms is not UNSET:
            field_dict["start_time_ms"] = start_time_ms
        if end_time_ms is not UNSET:
            field_dict["end_time_ms"] = end_time_ms
        if frame_index is not UNSET:
            field_dict["frame_index"] = frame_index
        if frame_delay_ms is not UNSET:
            field_dict["frame_delay_ms"] = frame_delay_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        data = d.pop("data", UNSET)

        start_time_ms = d.pop("start_time_ms", UNSET)

        end_time_ms = d.pop("end_time_ms", UNSET)

        frame_index = d.pop("frame_index", UNSET)

        frame_delay_ms = d.pop("frame_delay_ms", UNSET)

        inference_binary_content = cls(
            data=data,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            frame_index=frame_index,
            frame_delay_ms=frame_delay_ms,
        )

        inference_binary_content.additional_properties = d
        return inference_binary_content

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
