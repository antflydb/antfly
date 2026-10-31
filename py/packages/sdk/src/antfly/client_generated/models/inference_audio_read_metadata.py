from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_audio_read_metadata_language_source import InferenceAudioReadMetadataLanguageSource
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_audio_read_segment import InferenceAudioReadSegment


T = TypeVar("T", bound="InferenceAudioReadMetadata")


@_attrs_define
class InferenceAudioReadMetadata:
    """Recognition evidence for the audio track only. Video frames are not captioned. Absent segments and speakers make no
    alignment or speaker claim.

        Attributes:
            duration_ms (int | None): Decoded recording duration in milliseconds. Null when a remote recognition provider
                does not report duration.
            language (str | Unset):
            language_source (InferenceAudioReadMetadataLanguageSource | Unset):
            segments (list[InferenceAudioReadSegment] | Unset):
            speakers (list[str] | Unset):
    """

    duration_ms: int | None
    language: str | Unset = UNSET
    language_source: InferenceAudioReadMetadataLanguageSource | Unset = UNSET
    segments: list[InferenceAudioReadSegment] | Unset = UNSET
    speakers: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        duration_ms: int | None
        duration_ms = self.duration_ms

        language = self.language

        language_source: str | Unset = UNSET
        if not isinstance(self.language_source, Unset):
            language_source = self.language_source.value

        segments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.segments, Unset):
            segments = []
            for segments_item_data in self.segments:
                segments_item = segments_item_data.to_dict()
                segments.append(segments_item)

        speakers: list[str] | Unset = UNSET
        if not isinstance(self.speakers, Unset):
            speakers = self.speakers

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "duration_ms": duration_ms,
            }
        )
        if language is not UNSET:
            field_dict["language"] = language
        if language_source is not UNSET:
            field_dict["language_source"] = language_source
        if segments is not UNSET:
            field_dict["segments"] = segments
        if speakers is not UNSET:
            field_dict["speakers"] = speakers

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_audio_read_segment import InferenceAudioReadSegment

        d = dict(src_dict)

        def _parse_duration_ms(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        duration_ms = _parse_duration_ms(d.pop("duration_ms"))

        language = d.pop("language", UNSET)

        _language_source = d.pop("language_source", UNSET)
        language_source: InferenceAudioReadMetadataLanguageSource | Unset
        if isinstance(_language_source, Unset):
            language_source = UNSET
        else:
            language_source = InferenceAudioReadMetadataLanguageSource(_language_source)

        _segments = d.pop("segments", UNSET)
        segments: list[InferenceAudioReadSegment] | Unset = UNSET
        if _segments is not UNSET:
            segments = []
            for segments_item_data in _segments:
                segments_item = InferenceAudioReadSegment.from_dict(segments_item_data)

                segments.append(segments_item)

        speakers = cast(list[str], d.pop("speakers", UNSET))

        inference_audio_read_metadata = cls(
            duration_ms=duration_ms,
            language=language,
            language_source=language_source,
            segments=segments,
            speakers=speakers,
        )

        inference_audio_read_metadata.additional_properties = d
        return inference_audio_read_metadata

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
