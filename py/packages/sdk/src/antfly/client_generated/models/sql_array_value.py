from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.sql_array_dimension import SQLArrayDimension


T = TypeVar("T", bound="SQLArrayValue")


@_attrs_define
class SQLArrayValue:
    """Non-NULL SQL array result. Elements are flat, row-major values using the
    column's element_type. Their count equals the product of dimension
    lengths. Empty arrays have no dimensions and no elements. Integer
    elements are canonical decimal strings. Exact numeric elements are
    decimal strings preserving display scale, or NaN, Infinity and -Infinity.
    Floating elements are JSON
    numbers, or the strings NaN, Infinity and -Infinity. Element null flags
    distinguish SQL NULL from the JSON literal null in jsonb arrays. A NULL
    array is an outer null result cell, not an empty array or this envelope.

        Attributes:
            dimensions (list[SQLArrayDimension]):
            values (list[Any]):
            sql_nulls (list[bool]): Exactly one flag per value. True requires a null value; false permits a JSON null only
                for jsonb elements.
    """

    dimensions: list[SQLArrayDimension]
    values: list[Any]
    sql_nulls: list[bool]

    def to_dict(self) -> dict[str, Any]:
        dimensions = []
        for dimensions_item_data in self.dimensions:
            dimensions_item = dimensions_item_data.to_dict()
            dimensions.append(dimensions_item)

        values = self.values

        sql_nulls = self.sql_nulls

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "dimensions": dimensions,
                "values": values,
                "sql_nulls": sql_nulls,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_array_dimension import SQLArrayDimension

        d = dict(src_dict)
        dimensions = []
        _dimensions = d.pop("dimensions")
        for dimensions_item_data in _dimensions:
            dimensions_item = SQLArrayDimension.from_dict(dimensions_item_data)

            dimensions.append(dimensions_item)

        values = cast(list[Any], d.pop("values"))

        sql_nulls = cast(list[bool], d.pop("sql_nulls"))

        sql_array_value = cls(
            dimensions=dimensions,
            values=values,
            sql_nulls=sql_nulls,
        )

        return sql_array_value
