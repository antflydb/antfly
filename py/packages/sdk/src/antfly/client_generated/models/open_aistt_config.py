from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="OpenAISTTConfig")


@_attrs_define
class OpenAISTTConfig:
    """Configuration for OpenAI STT (Whisper) provider.

    API key via `api_key` field or `OPENAI_API_KEY` environment variable.

    **Model:** whisper-1

    **Supported Formats:** mp3, wav, webm, ogg, flac (max 25MB)

    **Docs:** https://platform.openai.com/docs/guides/speech-to-text

        Example:
            {'model': 'whisper-1'}

        Attributes:
            model (str | Unset): Whisper model to use. Default: 'whisper-1'.
            api_key (str | Unset): OpenAI API key. Falls back to OPENAI_API_KEY environment variable.
            base_url (str | Unset): API base URL. Falls back to OPENAI_BASE_URL environment variable.
    """

    model: str | Unset = "whisper-1"
    api_key: str | Unset = UNSET
    base_url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        api_key = self.api_key

        base_url = self.base_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if model is not UNSET:
            field_dict["model"] = model
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if base_url is not UNSET:
            field_dict["base_url"] = base_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model", UNSET)

        api_key = d.pop("api_key", UNSET)

        base_url = d.pop("base_url", UNSET)

        open_aistt_config = cls(
            model=model,
            api_key=api_key,
            base_url=base_url,
        )

        open_aistt_config.additional_properties = d
        return open_aistt_config

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
