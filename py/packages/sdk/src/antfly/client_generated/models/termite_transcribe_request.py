from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TermiteTranscribeRequest")


@_attrs_define
class TermiteTranscribeRequest:
    """
    Attributes:
        audio (str): Base64-encoded audio data (WAV, MP3, FLAC, etc.)
        model (str | Unset): Name of transcriber model from models_dir/transcribers/ Example: openai/whisper-tiny.
        language (str | Unset): Force specific language for transcription (optional, model-dependent) Example: en.
    """

    audio: str
    model: str | Unset = UNSET
    language: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        audio = self.audio

        model = self.model

        language = self.language

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "audio": audio,
            }
        )
        if model is not UNSET:
            field_dict["model"] = model
        if language is not UNSET:
            field_dict["language"] = language

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        audio = d.pop("audio")

        model = d.pop("model", UNSET)

        language = d.pop("language", UNSET)

        termite_transcribe_request = cls(
            audio=audio,
            model=model,
            language=language,
        )

        termite_transcribe_request.additional_properties = d
        return termite_transcribe_request

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
