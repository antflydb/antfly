from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.level import Level
from ..models.style import Style
from ..types import UNSET, Unset

T = TypeVar("T", bound="Config")


@_attrs_define
class Config:
    """Logging configuration for Antfly inference services

    Attributes:
        level (Level | Unset): Logging verbosity level Example: info.
        style (Style | Unset): Logging output format style. 'terminal' for colorized console, 'json' for structured
            JSON, 'logfmt' for token-efficient key=value pairs, 'noop' for silent. Example: terminal.
    """

    level: Level | Unset = UNSET
    style: Style | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        level: str | Unset = UNSET
        if not isinstance(self.level, Unset):
            level = self.level.value

        style: str | Unset = UNSET
        if not isinstance(self.style, Unset):
            style = self.style.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if level is not UNSET:
            field_dict["level"] = level
        if style is not UNSET:
            field_dict["style"] = style

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _level = d.pop("level", UNSET)
        level: Level | Unset
        if isinstance(_level, Unset):
            level = UNSET
        else:
            level = Level(_level)

        _style = d.pop("style", UNSET)
        style: Style | Unset
        if isinstance(_style, Unset):
            style = UNSET
        else:
            style = Style(_style)

        config = cls(
            level=level,
            style=style,
        )

        config.additional_properties = d
        return config

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
