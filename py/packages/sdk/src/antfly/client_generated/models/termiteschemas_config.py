from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_level import TermiteLevel
from ..models.termite_style import TermiteStyle
from ..types import UNSET, Unset

T = TypeVar("T", bound="TermiteschemasConfig")


@_attrs_define
class TermiteschemasConfig:
    """Logging configuration for Termite services

    Attributes:
        level (TermiteLevel | Unset): Logging verbosity level Example: info.
        style (TermiteStyle | Unset): Logging output format style. 'terminal' for colorized console, 'json' for
            structured JSON, 'logfmt' for token-efficient key=value pairs, 'noop' for silent. Example: terminal.
    """

    level: TermiteLevel | Unset = UNSET
    style: TermiteStyle | Unset = UNSET
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
        level: TermiteLevel | Unset
        if isinstance(_level, Unset):
            level = UNSET
        else:
            level = TermiteLevel(_level)

        _style = d.pop("style", UNSET)
        style: TermiteStyle | Unset
        if isinstance(_style, Unset):
            style = UNSET
        else:
            style = TermiteStyle(_style)

        termiteschemas_config = cls(
            level=level,
            style=style,
        )

        termiteschemas_config.additional_properties = d
        return termiteschemas_config

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
