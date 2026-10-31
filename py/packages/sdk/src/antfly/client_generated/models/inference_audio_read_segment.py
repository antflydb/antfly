from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_audio_read_segment_granularity import InferenceAudioReadSegmentGranularity
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceAudioReadSegment")


@_attrs_define
class InferenceAudioReadSegment:
    """
    Attributes:
        text (str):
        start_ms (int): Start in milliseconds relative to the original recording.
        end_ms (int): Exclusive end in milliseconds relative to the original recording.
        text_start (int): Start UTF-8 byte offset in the containing read result text.
        text_end (int): Exclusive end UTF-8 byte offset in the containing read result text.
        granularity (InferenceAudioReadSegmentGranularity | Unset): phrase uses recognizer-supplied phrase clocks;
            window provides only the original recognition-window bounds. Neither claims word alignment. Absent means
            unspecified.
        speaker (str | Unset): Speaker label when diarization was requested and established.
    """

    text: str
    start_ms: int
    end_ms: int
    text_start: int
    text_end: int
    granularity: InferenceAudioReadSegmentGranularity | Unset = UNSET
    speaker: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        start_ms = self.start_ms

        end_ms = self.end_ms

        text_start = self.text_start

        text_end = self.text_end

        granularity: str | Unset = UNSET
        if not isinstance(self.granularity, Unset):
            granularity = self.granularity.value

        speaker = self.speaker

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "text_start": text_start,
                "text_end": text_end,
            }
        )
        if granularity is not UNSET:
            field_dict["granularity"] = granularity
        if speaker is not UNSET:
            field_dict["speaker"] = speaker

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        text = d.pop("text")

        start_ms = d.pop("start_ms")

        end_ms = d.pop("end_ms")

        text_start = d.pop("text_start")

        text_end = d.pop("text_end")

        _granularity = d.pop("granularity", UNSET)
        granularity: InferenceAudioReadSegmentGranularity | Unset
        if isinstance(_granularity, Unset):
            granularity = UNSET
        else:
            granularity = InferenceAudioReadSegmentGranularity(_granularity)

        speaker = d.pop("speaker", UNSET)

        inference_audio_read_segment = cls(
            text=text,
            start_ms=start_ms,
            end_ms=end_ms,
            text_start=text_start,
            text_end=text_end,
            granularity=granularity,
            speaker=speaker,
        )

        inference_audio_read_segment.additional_properties = d
        return inference_audio_read_segment

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
