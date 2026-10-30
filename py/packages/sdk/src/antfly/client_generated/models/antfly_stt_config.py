from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AntflySTTConfig")


@_attrs_define
class AntflySTTConfig:
    """Configuration for Antfly inference STT (Whisper, Wav2Vec2, HuBERT) provider.

    Uses the Antfly inference service for speech-to-text inference.

    **Supported Models:** openai/whisper-tiny, openai/whisper-base, facebook/wav2vec2-base

    **Supported Formats:** WAV (recommended), MP3, FLAC, M4A/AAC

    **Docs:** See inference documentation

        Example:
            {'api_url': 'http://localhost:8080', 'model': 'openai/whisper-base'}

        Attributes:
            model (str): Explicit Antfly transcriber model name (e.g., 'openai/whisper-tiny').
            api_url (str | Unset): Inference API URL. Falls back to ANTFLY_INFERENCE_URL environment variable.
    """

    model: str
    api_url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        api_url = self.api_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if api_url is not UNSET:
            field_dict["api_url"] = api_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        api_url = d.pop("api_url", UNSET)

        antfly_stt_config = cls(
            model=model,
            api_url=api_url,
        )

        antfly_stt_config.additional_properties = d
        return antfly_stt_config

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
