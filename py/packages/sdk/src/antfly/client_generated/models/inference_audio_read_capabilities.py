from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceAudioReadCapabilities")


@_attrs_define
class InferenceAudioReadCapabilities:
    """
    Attributes:
        forced_language (bool):
        detected_language (bool):
        timestamps (bool):
        diarization (bool):
        windowing (bool):
        max_duration_ms (int | None | Unset): Maximum duration for this backend in milliseconds. Null means windowed
            recognition bounded by source and execution budgets.
    """

    forced_language: bool
    detected_language: bool
    timestamps: bool
    diarization: bool
    windowing: bool
    max_duration_ms: int | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        forced_language = self.forced_language

        detected_language = self.detected_language

        timestamps = self.timestamps

        diarization = self.diarization

        windowing = self.windowing

        max_duration_ms: int | None | Unset
        if isinstance(self.max_duration_ms, Unset):
            max_duration_ms = UNSET
        else:
            max_duration_ms = self.max_duration_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "forced_language": forced_language,
                "detected_language": detected_language,
                "timestamps": timestamps,
                "diarization": diarization,
                "windowing": windowing,
            }
        )
        if max_duration_ms is not UNSET:
            field_dict["max_duration_ms"] = max_duration_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        forced_language = d.pop("forced_language")

        detected_language = d.pop("detected_language")

        timestamps = d.pop("timestamps")

        diarization = d.pop("diarization")

        windowing = d.pop("windowing")

        def _parse_max_duration_ms(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        max_duration_ms = _parse_max_duration_ms(d.pop("max_duration_ms", UNSET))

        inference_audio_read_capabilities = cls(
            forced_language=forced_language,
            detected_language=detected_language,
            timestamps=timestamps,
            diarization=diarization,
            windowing=windowing,
            max_duration_ms=max_duration_ms,
        )

        inference_audio_read_capabilities.additional_properties = d
        return inference_audio_read_capabilities

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
