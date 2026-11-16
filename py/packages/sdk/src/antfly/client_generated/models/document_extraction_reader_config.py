from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_extraction_reader_config_provider import DocumentExtractionReaderConfigProvider
from ..models.document_extraction_reader_config_recognition_level import DocumentExtractionReaderConfigRecognitionLevel
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentExtractionReaderConfig")


@_attrs_define
class DocumentExtractionReaderConfig:
    """Reader provider for OCR. Every provider except `apple` requires `model`; Apple uses Vision text recognition and
    accepts no model, credentials, or prompt.

        Attributes:
            provider (DocumentExtractionReaderConfigProvider):
            model (str | Unset): Reader model, such as `lightonai/LightOnOCR-3-0.8B`.
            prompt (str | Unset): Provider-level prompt. Hosted readers send their own read instruction when it is omitted.
            max_tokens (int | Unset): Maximum output tokens per page.
            api_url (str | Unset): Reader endpoint URL.
            url (str | Unset): Reader endpoint URL; takes precedence over `api_url`.
            base_url (str | Unset): Provider API base URL.
            api_key (str | Unset): API key, normally a `${secret:...}` reference.
            bearer_token (str | Unset): Bearer token, normally a `${secret:...}` reference.
            capability_token (str | Unset): Capability token for an Antfly inference endpoint.
            capability_revision (str | Unset): Revision of the capability token.
            account_id (str | Unset): Cloudflare account ID. Required for `cloudflare`.
            gateway_id (str | Unset): Cloudflare AI Gateway ID.
            project_id (str | Unset): Google Cloud project for `vertex`.
            location (str | Unset): Google Cloud location for `vertex`.
            credentials_path (str | Unset): Path to Google Cloud credentials for `vertex`.
            recognition_languages (list[str] | Unset): Apple Vision recognition languages. Defaults to `en-US`.
            recognition_level (DocumentExtractionReaderConfigRecognitionLevel | Unset): Apple Vision recognition level.
                Default: DocumentExtractionReaderConfigRecognitionLevel.ACCURATE.
            uses_language_correction (bool | Unset): Apple Vision language correction. Default: False.
    """

    provider: DocumentExtractionReaderConfigProvider
    model: str | Unset = UNSET
    prompt: str | Unset = UNSET
    max_tokens: int | Unset = UNSET
    api_url: str | Unset = UNSET
    url: str | Unset = UNSET
    base_url: str | Unset = UNSET
    api_key: str | Unset = UNSET
    bearer_token: str | Unset = UNSET
    capability_token: str | Unset = UNSET
    capability_revision: str | Unset = UNSET
    account_id: str | Unset = UNSET
    gateway_id: str | Unset = UNSET
    project_id: str | Unset = UNSET
    location: str | Unset = UNSET
    credentials_path: str | Unset = UNSET
    recognition_languages: list[str] | Unset = UNSET
    recognition_level: DocumentExtractionReaderConfigRecognitionLevel | Unset = (
        DocumentExtractionReaderConfigRecognitionLevel.ACCURATE
    )
    uses_language_correction: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        model = self.model

        prompt = self.prompt

        max_tokens = self.max_tokens

        api_url = self.api_url

        url = self.url

        base_url = self.base_url

        api_key = self.api_key

        bearer_token = self.bearer_token

        capability_token = self.capability_token

        capability_revision = self.capability_revision

        account_id = self.account_id

        gateway_id = self.gateway_id

        project_id = self.project_id

        location = self.location

        credentials_path = self.credentials_path

        recognition_languages: list[str] | Unset = UNSET
        if not isinstance(self.recognition_languages, Unset):
            recognition_languages = self.recognition_languages

        recognition_level: str | Unset = UNSET
        if not isinstance(self.recognition_level, Unset):
            recognition_level = self.recognition_level.value

        uses_language_correction = self.uses_language_correction

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if model is not UNSET:
            field_dict["model"] = model
        if prompt is not UNSET:
            field_dict["prompt"] = prompt
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if api_url is not UNSET:
            field_dict["api_url"] = api_url
        if url is not UNSET:
            field_dict["url"] = url
        if base_url is not UNSET:
            field_dict["base_url"] = base_url
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if bearer_token is not UNSET:
            field_dict["bearer_token"] = bearer_token
        if capability_token is not UNSET:
            field_dict["capability_token"] = capability_token
        if capability_revision is not UNSET:
            field_dict["capability_revision"] = capability_revision
        if account_id is not UNSET:
            field_dict["account_id"] = account_id
        if gateway_id is not UNSET:
            field_dict["gateway_id"] = gateway_id
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if credentials_path is not UNSET:
            field_dict["credentials_path"] = credentials_path
        if recognition_languages is not UNSET:
            field_dict["recognition_languages"] = recognition_languages
        if recognition_level is not UNSET:
            field_dict["recognition_level"] = recognition_level
        if uses_language_correction is not UNSET:
            field_dict["uses_language_correction"] = uses_language_correction

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = DocumentExtractionReaderConfigProvider(d.pop("provider"))

        model = d.pop("model", UNSET)

        prompt = d.pop("prompt", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        api_url = d.pop("api_url", UNSET)

        url = d.pop("url", UNSET)

        base_url = d.pop("base_url", UNSET)

        api_key = d.pop("api_key", UNSET)

        bearer_token = d.pop("bearer_token", UNSET)

        capability_token = d.pop("capability_token", UNSET)

        capability_revision = d.pop("capability_revision", UNSET)

        account_id = d.pop("account_id", UNSET)

        gateway_id = d.pop("gateway_id", UNSET)

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        recognition_languages = cast(list[str], d.pop("recognition_languages", UNSET))

        _recognition_level = d.pop("recognition_level", UNSET)
        recognition_level: DocumentExtractionReaderConfigRecognitionLevel | Unset
        if isinstance(_recognition_level, Unset):
            recognition_level = UNSET
        else:
            recognition_level = DocumentExtractionReaderConfigRecognitionLevel(_recognition_level)

        uses_language_correction = d.pop("uses_language_correction", UNSET)

        document_extraction_reader_config = cls(
            provider=provider,
            model=model,
            prompt=prompt,
            max_tokens=max_tokens,
            api_url=api_url,
            url=url,
            base_url=base_url,
            api_key=api_key,
            bearer_token=bearer_token,
            capability_token=capability_token,
            capability_revision=capability_revision,
            account_id=account_id,
            gateway_id=gateway_id,
            project_id=project_id,
            location=location,
            credentials_path=credentials_path,
            recognition_languages=recognition_languages,
            recognition_level=recognition_level,
            uses_language_correction=uses_language_correction,
        )

        document_extraction_reader_config.additional_properties = d
        return document_extraction_reader_config

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
