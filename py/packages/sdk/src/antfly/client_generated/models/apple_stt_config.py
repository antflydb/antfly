from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AppleSTTConfig")


@_attrs_define
class AppleSTTConfig:
    """On-device SpeechAnalyzer file transcription. Requires a macOS Apple provider build and supported Speech assets.
    Diarization is unsupported.

        Attributes:
            language_code (str | Unset): Supported Speech locale; Apple transcription does not infer a language. Default:
                'en-US'.
            timestamps (bool | Unset):  Default: True.
            download_assets (bool | Unset): Explicitly permit preparing and installing the selected Speech assets. Default:
                False.
    """

    language_code: str | Unset = "en-US"
    timestamps: bool | Unset = True
    download_assets: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        language_code = self.language_code

        timestamps = self.timestamps

        download_assets = self.download_assets

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if language_code is not UNSET:
            field_dict["language_code"] = language_code
        if timestamps is not UNSET:
            field_dict["timestamps"] = timestamps
        if download_assets is not UNSET:
            field_dict["download_assets"] = download_assets

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        language_code = d.pop("language_code", UNSET)

        timestamps = d.pop("timestamps", UNSET)

        download_assets = d.pop("download_assets", UNSET)

        apple_stt_config = cls(
            language_code=language_code,
            timestamps=timestamps,
            download_assets=download_assets,
        )

        apple_stt_config.additional_properties = d
        return apple_stt_config

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
