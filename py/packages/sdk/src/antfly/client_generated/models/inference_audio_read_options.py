from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceAudioReadOptions")


@_attrs_define
class InferenceAudioReadOptions:
    """Recognition options, checked against every selected backend and fallback before inference. Whisper supports all
    options and pause-aware windowing of long recordings. Gemma supports transcription up to 30 seconds and rejects
    language, timestamps and diarization requests. No backend is substituted to satisfy an option.

        Attributes:
            language (str | Unset): Force recognition in this language (e.g. en). Omitted means detect language where
                supported; this is not a translation or cleanup request. Gemma does not support language control or detection
                metadata.
            timestamps (bool | Unset): Request phrase timestamps in milliseconds relative to the original source recording.
                Defaults to false. Does not imply word alignment.
            diarization (bool | Unset): Request speaker labels and temporal segments. Defaults to false. Whisper requires
                the local speaker embedding model; unavailable models cause an explicit error. Implies timestamp decoding.
    """

    language: str | Unset = UNSET
    timestamps: bool | Unset = UNSET
    diarization: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        language = self.language

        timestamps = self.timestamps

        diarization = self.diarization

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if language is not UNSET:
            field_dict["language"] = language
        if timestamps is not UNSET:
            field_dict["timestamps"] = timestamps
        if diarization is not UNSET:
            field_dict["diarization"] = diarization

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        language = d.pop("language", UNSET)

        timestamps = d.pop("timestamps", UNSET)

        diarization = d.pop("diarization", UNSET)

        inference_audio_read_options = cls(
            language=language,
            timestamps=timestamps,
            diarization=diarization,
        )

        return inference_audio_read_options
