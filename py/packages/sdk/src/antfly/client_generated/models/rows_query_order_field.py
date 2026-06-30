from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_query_order_field_direction import RowsQueryOrderFieldDirection
from ..models.rows_query_order_field_null_test import RowsQueryOrderFieldNullTest
from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsQueryOrderField")


@_attrs_define
class RowsQueryOrderField:
    """
    Attributes:
        field (str): Output/base field to order by. Mutually exclusive with `expr`.
        null_test (RowsQueryOrderFieldNullTest | Unset):
        direction (RowsQueryOrderFieldDirection | Unset):
    """

    field: str
    null_test: RowsQueryOrderFieldNullTest | Unset = UNSET
    direction: RowsQueryOrderFieldDirection | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        null_test: str | Unset = UNSET
        if not isinstance(self.null_test, Unset):
            null_test = self.null_test.value

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
            }
        )
        if null_test is not UNSET:
            field_dict["null_test"] = null_test
        if direction is not UNSET:
            field_dict["direction"] = direction

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        _null_test = d.pop("null_test", UNSET)
        null_test: RowsQueryOrderFieldNullTest | Unset
        if isinstance(_null_test, Unset):
            null_test = UNSET
        else:
            null_test = RowsQueryOrderFieldNullTest(_null_test)

        _direction = d.pop("direction", UNSET)
        direction: RowsQueryOrderFieldDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = RowsQueryOrderFieldDirection(_direction)

        rows_query_order_field = cls(
            field=field,
            null_test=null_test,
            direction=direction,
        )

        return rows_query_order_field
