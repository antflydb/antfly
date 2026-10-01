from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_dictation_segment import InferenceDictationSegment


T = TypeVar("T", bound="InferenceDictationTranscript")


@_attrs_define
class InferenceDictationTranscript:
    """
    Attributes:
        text (str): Raw transcript before cleanup.
        duration_ms (int): Decoded clip duration in milliseconds.
        segments (list[InferenceDictationSegment]): Timestamped phrases in clip order.
        language (str | Unset): Detected or forced language.
    """

    text: str
    duration_ms: int
    segments: list[InferenceDictationSegment]
    language: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        duration_ms = self.duration_ms

        segments = []
        for segments_item_data in self.segments:
            segments_item = segments_item_data.to_dict()
            segments.append(segments_item)

        language = self.language

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
                "duration_ms": duration_ms,
                "segments": segments,
            }
        )
        if language is not UNSET:
            field_dict["language"] = language

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_dictation_segment import InferenceDictationSegment

        d = dict(src_dict)
        text = d.pop("text")

        duration_ms = d.pop("duration_ms")

        segments = []
        _segments = d.pop("segments")
        for segments_item_data in _segments:
            segments_item = InferenceDictationSegment.from_dict(segments_item_data)

            segments.append(segments_item)

        language = d.pop("language", UNSET)

        inference_dictation_transcript = cls(
            text=text,
            duration_ms=duration_ms,
            segments=segments,
            language=language,
        )

        inference_dictation_transcript.additional_properties = d
        return inference_dictation_transcript

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
