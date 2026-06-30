from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_aggregate_result_set_rows_item import RowsAggregateResultSetRowsItem
    from ..models.rows_result_column import RowsResultColumn


T = TypeVar("T", bound="RowsAggregateResultSet")


@_attrs_define
class RowsAggregateResultSet:
    """
    Attributes:
        total_groups (int | Unset):
        result_schema (list[RowsResultColumn] | Unset):
        rows (list[RowsAggregateResultSetRowsItem] | Unset):
    """

    total_groups: int | Unset = UNSET
    result_schema: list[RowsResultColumn] | Unset = UNSET
    rows: list[RowsAggregateResultSetRowsItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        total_groups = self.total_groups

        result_schema: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.result_schema, Unset):
            result_schema = []
            for result_schema_item_data in self.result_schema:
                result_schema_item = result_schema_item_data.to_dict()
                result_schema.append(result_schema_item)

        rows: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.rows, Unset):
            rows = []
            for rows_item_data in self.rows:
                rows_item = rows_item_data.to_dict()
                rows.append(rows_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if total_groups is not UNSET:
            field_dict["total_groups"] = total_groups
        if result_schema is not UNSET:
            field_dict["result_schema"] = result_schema
        if rows is not UNSET:
            field_dict["rows"] = rows

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_aggregate_result_set_rows_item import RowsAggregateResultSetRowsItem
        from ..models.rows_result_column import RowsResultColumn

        d = dict(src_dict)
        total_groups = d.pop("total_groups", UNSET)

        _result_schema = d.pop("result_schema", UNSET)
        result_schema: list[RowsResultColumn] | Unset = UNSET
        if _result_schema is not UNSET:
            result_schema = []
            for result_schema_item_data in _result_schema:
                result_schema_item = RowsResultColumn.from_dict(result_schema_item_data)

                result_schema.append(result_schema_item)

        _rows = d.pop("rows", UNSET)
        rows: list[RowsAggregateResultSetRowsItem] | Unset = UNSET
        if _rows is not UNSET:
            rows = []
            for rows_item_data in _rows:
                rows_item = RowsAggregateResultSetRowsItem.from_dict(rows_item_data)

                rows.append(rows_item)

        rows_aggregate_result_set = cls(
            total_groups=total_groups,
            result_schema=result_schema,
            rows=rows,
        )

        rows_aggregate_result_set.additional_properties = d
        return rows_aggregate_result_set

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
