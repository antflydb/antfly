from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLNumericModifier")


@_attrs_define
class SQLNumericModifier:
    """PostgreSQL NUMERIC precision and signed scale. For arrays this describes every element, not dimensions. Absent means
    unconstrained NUMERIC.

        Attributes:
            precision (int):
            scale (int):
    """

    precision: int
    scale: int

    def to_dict(self) -> dict[str, Any]:
        precision = self.precision

        scale = self.scale

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "precision": precision,
                "scale": scale,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        precision = d.pop("precision")

        scale = d.pop("scale")

        sql_numeric_modifier = cls(
            precision=precision,
            scale=scale,
        )

        return sql_numeric_modifier
