from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_regex_validator_mode import ExtractionRegexValidatorMode
from ..models.extraction_regex_validator_type import ExtractionRegexValidatorType
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionRegexValidator")


@_attrs_define
class ExtractionRegexValidator:
    """
    Attributes:
        pattern (str):
        type_ (ExtractionRegexValidatorType | Unset):
        mode (ExtractionRegexValidatorMode | Unset):  Default: ExtractionRegexValidatorMode.FULL.
        exclude (bool | Unset):  Default: False.
        flags (int | Unset): Python-compatible regex flags supported by the active bounded validator engine; unsupported
            flags or syntax fail validation. Default: 2.
    """

    pattern: str
    type_: ExtractionRegexValidatorType | Unset = UNSET
    mode: ExtractionRegexValidatorMode | Unset = ExtractionRegexValidatorMode.FULL
    exclude: bool | Unset = False
    flags: int | Unset = 2

    def to_dict(self) -> dict[str, Any]:
        pattern = self.pattern

        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        exclude = self.exclude

        flags = self.flags

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "pattern": pattern,
            }
        )
        if type_ is not UNSET:
            field_dict["type"] = type_
        if mode is not UNSET:
            field_dict["mode"] = mode
        if exclude is not UNSET:
            field_dict["exclude"] = exclude
        if flags is not UNSET:
            field_dict["flags"] = flags

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        pattern = d.pop("pattern")

        _type_ = d.pop("type", UNSET)
        type_: ExtractionRegexValidatorType | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = ExtractionRegexValidatorType(_type_)

        _mode = d.pop("mode", UNSET)
        mode: ExtractionRegexValidatorMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = ExtractionRegexValidatorMode(_mode)

        exclude = d.pop("exclude", UNSET)

        flags = d.pop("flags", UNSET)

        extraction_regex_validator = cls(
            pattern=pattern,
            type_=type_,
            mode=mode,
            exclude=exclude,
            flags=flags,
        )

        return extraction_regex_validator
