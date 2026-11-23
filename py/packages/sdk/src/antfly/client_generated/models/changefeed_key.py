from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.changefeed_key_encoding import ChangefeedKeyEncoding

T = TypeVar("T", bound="ChangefeedKey")


@_attrs_define
class ChangefeedKey:
    """
    Attributes:
        encoding (ChangefeedKeyEncoding):
        value (str):
    """

    encoding: ChangefeedKeyEncoding
    value: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        encoding = self.encoding.value

        value = self.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "encoding": encoding,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        encoding = ChangefeedKeyEncoding(d.pop("encoding"))

        value = d.pop("value")

        changefeed_key = cls(
            encoding=encoding,
            value=value,
        )

        changefeed_key.additional_properties = d
        return changefeed_key

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
