from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceVADOptions")


@_attrs_define
class InferenceVADOptions:
    """Options for Voice Activity Detection (VAD) based audio segmentation. inference-specific.

    Attributes:
        min_silence_duration_ms (int | Unset): Minimum silence duration (ms) to split speech segments. Gaps shorter than
            this are merged. Higher values produce longer, fewer segments. Default: 300.
        min_speech_duration_ms (int | Unset): Minimum speech duration (ms) for a segment to be kept. Shorter segments
            are discarded. Default: 250.
        speech_pad_ms (int | Unset): Padding (ms) added before and after detected speech. Default: 30.
        max_segment_duration_ms (int | Unset): Maximum segment duration (ms). Segments longer than this are split.
            Useful for Whisper-compatible chunking. Default: 30000.
    """

    min_silence_duration_ms: int | Unset = UNSET
    min_speech_duration_ms: int | Unset = UNSET
    speech_pad_ms: int | Unset = UNSET
    max_segment_duration_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        min_silence_duration_ms = self.min_silence_duration_ms

        min_speech_duration_ms = self.min_speech_duration_ms

        speech_pad_ms = self.speech_pad_ms

        max_segment_duration_ms = self.max_segment_duration_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if min_silence_duration_ms is not UNSET:
            field_dict["min_silence_duration_ms"] = min_silence_duration_ms
        if min_speech_duration_ms is not UNSET:
            field_dict["min_speech_duration_ms"] = min_speech_duration_ms
        if speech_pad_ms is not UNSET:
            field_dict["speech_pad_ms"] = speech_pad_ms
        if max_segment_duration_ms is not UNSET:
            field_dict["max_segment_duration_ms"] = max_segment_duration_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        min_silence_duration_ms = d.pop("min_silence_duration_ms", UNSET)

        min_speech_duration_ms = d.pop("min_speech_duration_ms", UNSET)

        speech_pad_ms = d.pop("speech_pad_ms", UNSET)

        max_segment_duration_ms = d.pop("max_segment_duration_ms", UNSET)

        inference_vad_options = cls(
            min_silence_duration_ms=min_silence_duration_ms,
            min_speech_duration_ms=min_speech_duration_ms,
            speech_pad_ms=speech_pad_ms,
            max_segment_duration_ms=max_segment_duration_ms,
        )

        inference_vad_options.additional_properties = d
        return inference_vad_options

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
