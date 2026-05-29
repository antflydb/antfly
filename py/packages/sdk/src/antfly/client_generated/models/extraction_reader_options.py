from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionReaderOptions")


@_attrs_define
class ExtractionReaderOptions:
    """
    Attributes:
        provider (str | Unset):
        model (str | Unset):
        url (str | Unset):
        api_url (str | Unset):
    """

    provider: str | Unset = UNSET
    model: str | Unset = UNSET
    url: str | Unset = UNSET
    api_url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider

        model = self.model

        url = self.url

        api_url = self.api_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if provider is not UNSET:
            field_dict["provider"] = provider
        if model is not UNSET:
            field_dict["model"] = model
        if url is not UNSET:
            field_dict["url"] = url
        if api_url is not UNSET:
            field_dict["api_url"] = api_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = d.pop("provider", UNSET)

        model = d.pop("model", UNSET)

        url = d.pop("url", UNSET)

        api_url = d.pop("api_url", UNSET)

        extraction_reader_options = cls(
            provider=provider,
            model=model,
            url=url,
            api_url=api_url,
        )

        extraction_reader_options.additional_properties = d
        return extraction_reader_options

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
