from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="ComposedTableSource")


@_attrs_define
class ComposedTableSource:
    """
    Attributes:
        table (str): Literal native table name. Each source is independently authorized.
    """

    table: str

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "table": table,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table")

        composed_table_source = cls(
            table=table,
        )

        return composed_table_source
