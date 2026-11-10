from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="VertexSTTConfig")


@_attrs_define
class VertexSTTConfig:
    """Configuration for Google Cloud Speech-to-Text provider (Vertex AI).

    Uses Application Default Credentials (ADC) for authentication.

    **Features:** Streaming, speaker diarization, automatic punctuation

    **Docs:** https://cloud.google.com/speech-to-text/docs

        Example:
            {'project_id': 'my-gcp-project', 'language_code': 'en-US', 'enable_automatic_punctuation': True}

        Attributes:
            project_id (str | Unset): Google Cloud project ID. Falls back to GOOGLE_CLOUD_PROJECT environment variable.
            location (str | Unset): Google Cloud location. Default: 'us-central1'.
            credentials_path (str | Unset): Path to an ADC credential JSON file (service-account, authorized-user, or
                external-account). Falls back to the default ADC chain.
            language_code (str | Unset): Default language code (e.g., 'en-US', 'es-ES'). Default: 'en-US'.
            enable_automatic_punctuation (bool | Unset): Enable automatic punctuation. Default: True.
            use_enhanced (bool | Unset): Use enhanced models for better accuracy (costs more). Default: False.
            model (str | Unset): Recognition model (e.g., 'latest_long', 'telephony', 'medical_dictation').
    """

    project_id: str | Unset = UNSET
    location: str | Unset = "us-central1"
    credentials_path: str | Unset = UNSET
    language_code: str | Unset = "en-US"
    enable_automatic_punctuation: bool | Unset = True
    use_enhanced: bool | Unset = False
    model: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        project_id = self.project_id

        location = self.location

        credentials_path = self.credentials_path

        language_code = self.language_code

        enable_automatic_punctuation = self.enable_automatic_punctuation

        use_enhanced = self.use_enhanced

        model = self.model

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if credentials_path is not UNSET:
            field_dict["credentials_path"] = credentials_path
        if language_code is not UNSET:
            field_dict["language_code"] = language_code
        if enable_automatic_punctuation is not UNSET:
            field_dict["enable_automatic_punctuation"] = enable_automatic_punctuation
        if use_enhanced is not UNSET:
            field_dict["use_enhanced"] = use_enhanced
        if model is not UNSET:
            field_dict["model"] = model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        language_code = d.pop("language_code", UNSET)

        enable_automatic_punctuation = d.pop("enable_automatic_punctuation", UNSET)

        use_enhanced = d.pop("use_enhanced", UNSET)

        model = d.pop("model", UNSET)

        vertex_stt_config = cls(
            project_id=project_id,
            location=location,
            credentials_path=credentials_path,
            language_code=language_code,
            enable_automatic_punctuation=enable_automatic_punctuation,
            use_enhanced=use_enhanced,
            model=model,
        )

        vertex_stt_config.additional_properties = d
        return vertex_stt_config

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
