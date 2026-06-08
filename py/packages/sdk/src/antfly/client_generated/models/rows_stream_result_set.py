from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_stream_result_set_rows_item import RowsStreamResultSetRowsItem


T = TypeVar("T", bound="RowsStreamResultSet")


@_attrs_define
class RowsStreamResultSet:
    """
    Attributes:
        total_rows (int | Unset):
        rows (list[RowsStreamResultSetRowsItem] | Unset):
    """

    total_rows: int | Unset = UNSET
    rows: list[RowsStreamResultSetRowsItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        total_rows = self.total_rows

        rows: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.rows, Unset):
            rows = []
            for rows_item_data in self.rows:
                rows_item = rows_item_data.to_dict()
                rows.append(rows_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if total_rows is not UNSET:
            field_dict["total_rows"] = total_rows
        if rows is not UNSET:
            field_dict["rows"] = rows

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_stream_result_set_rows_item import RowsStreamResultSetRowsItem

        d = dict(src_dict)
        total_rows = d.pop("total_rows", UNSET)

        _rows = d.pop("rows", UNSET)
        rows: list[RowsStreamResultSetRowsItem] | Unset = UNSET
        if _rows is not UNSET:
            rows = []
            for rows_item_data in _rows:
                rows_item = RowsStreamResultSetRowsItem.from_dict(rows_item_data)

                rows.append(rows_item)

        rows_stream_result_set = cls(
            total_rows=total_rows,
            rows=rows,
        )

        rows_stream_result_set.additional_properties = d
        return rows_stream_result_set

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
