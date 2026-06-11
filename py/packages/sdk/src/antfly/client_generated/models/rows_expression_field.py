from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_expression_field_source import RowsExpressionFieldSource
from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsExpressionField")


@_attrs_define
class RowsExpressionField:
    """
    Attributes:
        field (str):
        source (RowsExpressionFieldSource | Unset):
    """

    field: str
    source: RowsExpressionFieldSource | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        source: str | Unset = UNSET
        if not isinstance(self.source, Unset):
            source = self.source.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
            }
        )
        if source is not UNSET:
            field_dict["source"] = source

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        _source = d.pop("source", UNSET)
        source: RowsExpressionFieldSource | Unset
        if isinstance(_source, Unset):
            source = UNSET
        else:
            source = RowsExpressionFieldSource(_source)

        rows_expression_field = cls(
            field=field,
            source=source,
        )

        return rows_expression_field
