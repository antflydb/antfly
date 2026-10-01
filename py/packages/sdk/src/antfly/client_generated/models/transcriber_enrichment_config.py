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

    Carries the provider's STT configuration (`provider`, `model`, `api_url`, `api_key`, ...) plus the transcription
    options below. The fields are declared inline rather than composed from `STTConfig` so that a generated client can
    leave an option out: a composed schema makes a typed client serialize every field, and a `max_download_bytes` of
    zero would reject every recording.

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
            model (str | Unset): Model name, as the provider names it (e.g. 'openai/whisper-base' for antfly, 'whisper-1'
                for openai).
            api_url (str | Unset): Antfly inference API URL. Falls back to ANTFLY_INFERENCE_URL.
            base_url (str | Unset): OpenAI API base URL. Falls back to OPENAI_BASE_URL.
            api_key (str | Unset): Provider API key. Falls back to the provider's environment variable.
            project_id (str | Unset): Google Cloud project ID for the vertex provider. Falls back to GOOGLE_CLOUD_PROJECT.
            location (str | Unset): Google Cloud location for the vertex provider.
            credentials_path (str | Unset): Path to an ADC credential JSON file for the vertex provider. Falls back to the
                default ADC chain.
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
    model: str | Unset = UNSET
    api_url: str | Unset = UNSET
    base_url: str | Unset = UNSET
    api_key: str | Unset = UNSET
    project_id: str | Unset = UNSET
    location: str | Unset = UNSET
    credentials_path: str | Unset = UNSET
    language_code: str | Unset = UNSET
    timestamps: bool | Unset = True
    diarization: bool | Unset = False
    max_download_bytes: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        model = self.model

        api_url = self.api_url

        base_url = self.base_url

        api_key = self.api_key

        project_id = self.project_id

        location = self.location

        credentials_path = self.credentials_path

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
        if model is not UNSET:
            field_dict["model"] = model
        if api_url is not UNSET:
            field_dict["api_url"] = api_url
        if base_url is not UNSET:
            field_dict["base_url"] = base_url
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if credentials_path is not UNSET:
            field_dict["credentials_path"] = credentials_path
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

        model = d.pop("model", UNSET)

        api_url = d.pop("api_url", UNSET)

        base_url = d.pop("base_url", UNSET)

        api_key = d.pop("api_key", UNSET)

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        language_code = d.pop("language_code", UNSET)

        timestamps = d.pop("timestamps", UNSET)

        diarization = d.pop("diarization", UNSET)

        max_download_bytes = d.pop("max_download_bytes", UNSET)

        transcriber_enrichment_config = cls(
            provider=provider,
            model=model,
            api_url=api_url,
            base_url=base_url,
            api_key=api_key,
            project_id=project_id,
            location=location,
            credentials_path=credentials_path,
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
