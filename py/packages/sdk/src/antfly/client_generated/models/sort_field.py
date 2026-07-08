from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="SortField")


@_attrs_define
class SortField:
    """A single sort field with direction.

    Attributes:
        field (str): The field name to sort by.
        desc (bool | Unset): Sort direction. true = descending, false = ascending. Default: False.
    """

    field: str
    desc: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        desc = self.desc

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
            }
        )
        if desc is not UNSET:
            field_dict["desc"] = desc

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        desc = d.pop("desc", UNSET)

        sort_field = cls(
            field=field,
            desc=desc,
        )

        return sort_field
