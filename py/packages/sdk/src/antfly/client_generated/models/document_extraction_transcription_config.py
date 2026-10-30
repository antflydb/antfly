from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.stt_config import STTConfig


T = TypeVar("T", bound="DocumentExtractionTranscriptionConfig")


@_attrs_define
class DocumentExtractionTranscriptionConfig:
    """Transcribes audio units.

    Attributes:
        transcriber (STTConfig): Unified configuration for an STT provider.

            Select the provider type and configure provider-specific settings.

            **Supported Providers:**
            - `openai` - OpenAI Whisper (whisper-1)
            - `vertex` - Google Cloud Speech-to-Text (Vertex AI)
            - `antfly` - Antfly inference service (Whisper, Wav2Vec2, HuBERT)

            **Example:**
            ```yaml
            provider: antfly
            api_url: "http://localhost:8080"
            model: openai/whisper-base
            ``` Example: {'provider': 'antfly', 'api_url': 'http://localhost:8080', 'model': 'openai/whisper-base'}.
    """

    transcriber: STTConfig
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        transcriber = self.transcriber.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "transcriber": transcriber,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.stt_config import STTConfig

        d = dict(src_dict)
        transcriber = STTConfig.from_dict(d.pop("transcriber"))

        document_extraction_transcription_config = cls(
            transcriber=transcriber,
        )

        document_extraction_transcription_config.additional_properties = d
        return document_extraction_transcription_config

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
