from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.row_filter_entry_filter import RowFilterEntryFilter


T = TypeVar("T", bound="RowFilterEntry")


@_attrs_define
class RowFilterEntry:
    """A row filter policy for a user on a specific table.

    Attributes:
        table (str): Table name (or '*' for all tables). Example: orders.
        filter_ (RowFilterEntryFilter): Antfly query JSON that documents must match to be visible. Example: {'term':
            {'department': 'engineering'}}.
    """

    table: str
    filter_: RowFilterEntryFilter
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        filter_ = self.filter_.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
                "filter": filter_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.row_filter_entry_filter import RowFilterEntryFilter

        d = dict(src_dict)
        table = d.pop("table")

        filter_ = RowFilterEntryFilter.from_dict(d.pop("filter"))

        row_filter_entry = cls(
            table=table,
            filter_=filter_,
        )

        row_filter_entry.additional_properties = d
        return row_filter_entry

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
