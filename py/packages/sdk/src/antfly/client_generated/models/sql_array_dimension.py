from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLArrayDimension")


@_attrs_define
class SQLArrayDimension:
    """
    Attributes:
        length (int):
        lower_bound (int):
    """

    length: int
    lower_bound: int

    def to_dict(self) -> dict[str, Any]:
        length = self.length

        lower_bound = self.lower_bound

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "length": length,
                "lower_bound": lower_bound,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        length = d.pop("length")

        lower_bound = d.pop("lower_bound")

        sql_array_dimension = cls(
            length=length,
            lower_bound=lower_bound,
        )

        return sql_array_dimension
