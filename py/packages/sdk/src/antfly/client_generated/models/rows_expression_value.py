from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RowsExpressionValue")


@_attrs_define
class RowsExpressionValue:
    """
    Attributes:
        value (Any): Literal JSON value for a value node.
    """

    value: Any

    def to_dict(self) -> dict[str, Any]:
        value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        value = d.pop("value")

        rows_expression_value = cls(
            value=value,
        )

        return rows_expression_value
