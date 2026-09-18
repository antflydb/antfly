from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.stt_provider import STTProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="TranscriberEnrichmentConfig")


@_attrs_define
class TranscriberEnrichmentConfig:
    """Speech-to-text provider for the `transcriber` enrichment shorthand.

    Accepts every field of the provider's STT configuration (`provider`, `model`, `api_url`, `api_key`, ...) plus the
    transcription options below.

    **Example:**
    ```yaml
    name: call_transcripts
    kind: asset
    field: recording_url
    transcriber:
      provider: antfly
      model: openai/whisper-base
      language_code: en
      timestamps: true
    ```

        Attributes:
            provider (STTProvider): The STT provider to use.
            language_code (str | Unset): Spoken language hint (ISO 639-1, e.g. 'en'). Omit for automatic detection where the
                provider supports it.
            timestamps (bool | Unset): Request timestamped transcript segments so chunks carry recording offsets. Providers
                without segment timing return plain text. Default: True.
            diarization (bool | Unset): Request speaker labels on transcript segments where the provider supports them.
                Default: False.
            max_download_bytes (int | Unset): Largest recording fetched from a URL, in bytes. Defaults to 128 MiB, which
                covers a one hour voice memo or podcast.
    """

    provider: STTProvider
    language_code: str | Unset = UNSET
    timestamps: bool | Unset = True
    diarization: bool | Unset = False
    max_download_bytes: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        language_code = self.language_code

        timestamps = self.timestamps

        diarization = self.diarization

        max_download_bytes = self.max_download_bytes

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if language_code is not UNSET:
            field_dict["language_code"] = language_code
        if timestamps is not UNSET:
            field_dict["timestamps"] = timestamps
        if diarization is not UNSET:
            field_dict["diarization"] = diarization
        if max_download_bytes is not UNSET:
            field_dict["max_download_bytes"] = max_download_bytes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = STTProvider(d.pop("provider"))

        language_code = d.pop("language_code", UNSET)

        timestamps = d.pop("timestamps", UNSET)

        diarization = d.pop("diarization", UNSET)

        max_download_bytes = d.pop("max_download_bytes", UNSET)

        transcriber_enrichment_config = cls(
            provider=provider,
            language_code=language_code,
            timestamps=timestamps,
            diarization=diarization,
            max_download_bytes=max_download_bytes,
        )

        transcriber_enrichment_config.additional_properties = d
        return transcriber_enrichment_config

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
