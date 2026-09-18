from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_transcribe_object_object import InferenceTranscribeObjectObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_dictation_segment import InferenceDictationSegment


T = TypeVar("T", bound="InferenceTranscribeObject")


@_attrs_define
class InferenceTranscribeObject:
    """
    Attributes:
        object_ (InferenceTranscribeObjectObject):
        index (int): Input audio index.
        text (str): Transcribed text from the audio Example: Hello, how are you today?.
        language (str | Unset): Detected or forced language Example: en.
        duration_ms (int | Unset): Decoded clip duration in milliseconds.
        segments (list[InferenceDictationSegment] | Unset): Timestamped phrases in clip order, so a transcript can be
            indexed and linked back to a moment in the recording. Clips longer than 30 s are transcribed in windows; segment
            offsets are relative to the whole clip.
    """

    object_: InferenceTranscribeObjectObject
    index: int
    text: str
    language: str | Unset = UNSET
    duration_ms: int | Unset = UNSET
    segments: list[InferenceDictationSegment] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        index = self.index

        text = self.text

        language = self.language

        duration_ms = self.duration_ms

        segments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.segments, Unset):
            segments = []
            for segments_item_data in self.segments:
                segments_item = segments_item_data.to_dict()
                segments.append(segments_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "index": index,
                "text": text,
            }
        )
        if language is not UNSET:
            field_dict["language"] = language
        if duration_ms is not UNSET:
            field_dict["duration_ms"] = duration_ms
        if segments is not UNSET:
            field_dict["segments"] = segments

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_dictation_segment import InferenceDictationSegment

        d = dict(src_dict)
        object_ = InferenceTranscribeObjectObject(d.pop("object"))

        index = d.pop("index")

        text = d.pop("text")

        language = d.pop("language", UNSET)

        duration_ms = d.pop("duration_ms", UNSET)

        _segments = d.pop("segments", UNSET)
        segments: list[InferenceDictationSegment] | Unset = UNSET
        if _segments is not UNSET:
            segments = []
            for segments_item_data in _segments:
                segments_item = InferenceDictationSegment.from_dict(segments_item_data)

                segments.append(segments_item)

        inference_transcribe_object = cls(
            object_=object_,
            index=index,
            text=text,
            language=language,
            duration_ms=duration_ms,
            segments=segments,
        )

        inference_transcribe_object.additional_properties = d
        return inference_transcribe_object

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
