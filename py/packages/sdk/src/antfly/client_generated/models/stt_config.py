from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.stt_provider import STTProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="STTConfig")


@_attrs_define
class STTConfig:
    """Unified configuration for an STT provider.

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
    ```

        Example:
            {'provider': 'antfly', 'api_url': 'http://localhost:8080', 'model': 'openai/whisper-base'}

        Attributes:
            provider (STTProvider): The STT provider to use.
            max_total_tokens (int | Unset): Provider-declared hard per-call token bound. Required for hosted document
                reading when measurements are unavailable; the full allowance is charged against the document budget, never as
                measured usage.
    """

    provider: STTProvider
    max_total_tokens: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        max_total_tokens = self.max_total_tokens

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if max_total_tokens is not UNSET:
            field_dict["max_total_tokens"] = max_total_tokens

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = STTProvider(d.pop("provider"))

        max_total_tokens = d.pop("max_total_tokens", UNSET)

        stt_config = cls(
            provider=provider,
            max_total_tokens=max_total_tokens,
        )

        stt_config.additional_properties = d
        return stt_config

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
