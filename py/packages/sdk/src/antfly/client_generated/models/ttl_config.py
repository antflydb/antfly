from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="TtlConfig")


@_attrs_define
class TtlConfig:
    """Automatic document-expiration policy for a table.

    Attributes:
        duration (str): Expiration duration using Antfly's integer-component duration format.
            Supported units are `ns`, `us`, `ms`, `s`, `m`, `h`, and `d`;
            examples include `90m`, `1h30m`, and `7d`.
        field (str | Unset): Timestamp field used as the expiration reference. Default: '_timestamp'.
    """

    duration: str
    field: str | Unset = "_timestamp"

    def to_dict(self) -> dict[str, Any]:
        duration = self.duration

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "duration": duration,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        duration = d.pop("duration")

        field = d.pop("field", UNSET)

        ttl_config = cls(
            duration=duration,
            field=field,
        )

        return ttl_config
