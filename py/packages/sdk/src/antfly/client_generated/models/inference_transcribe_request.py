from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceTranscribeRequest")


@_attrs_define
class InferenceTranscribeRequest:
    """
    Attributes:
        model (str): Explicit name of the transcriber model from models_dir/transcribers/. Required so direct and
            distributed execution resolve the same model. Example: openai/whisper-tiny.
        audio (str): Base64-encoded audio data (WAV, MP3, AAC/M4A, MP4/MOV audio, Ogg/Opus, WebM/Matroska, FLAC, etc.).
            Clips longer than 30 s are transcribed in windows cut at pauses; silent clips return an empty transcript.
        language (str | Unset): Force specific language for transcription (optional, model-dependent) Example: en.
        diarization (bool | Unset): Label each segment with a speaker. Runs a local speaker-embedding model (pull
            `csukuangfj/speaker-embedding-models:3dspeaker_speech_campplus_sv_en_voxceleb_16k.onnx`) and clusters phrases by
            voice; labels are `SPEAKER_00`, `SPEAKER_01`, ... in order of first appearance.
    """

    model: str
    audio: str
    language: str | Unset = UNSET
    diarization: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        audio = self.audio

        language = self.language

        diarization = self.diarization

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "audio": audio,
            }
        )
        if language is not UNSET:
            field_dict["language"] = language
        if diarization is not UNSET:
            field_dict["diarization"] = diarization

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        audio = d.pop("audio")

        language = d.pop("language", UNSET)

        diarization = d.pop("diarization", UNSET)

        inference_transcribe_request = cls(
            model=model,
            audio=audio,
            language=language,
            diarization=diarization,
        )

        inference_transcribe_request.additional_properties = d
        return inference_transcribe_request

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
