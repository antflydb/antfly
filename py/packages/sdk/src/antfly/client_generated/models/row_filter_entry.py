from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.catalog_table_scope import CatalogTableScope
    from ..models.row_filter_entry_filter import RowFilterEntryFilter


T = TypeVar("T", bound="RowFilterEntry")


@_attrs_define
class RowFilterEntry:
    """A row filter policy for a user on a specific table.

    Attributes:
        table (str): Table name (or '*' for all tables). Example: orders.
        filter_ (RowFilterEntryFilter): Antfly query JSON that documents must match to be visible. Example: {'term':
            {'department': 'engineering'}}.
        table_target (CatalogTableScope | Unset): A table or all tables in an explicit namespace. A missing table
            selects the namespace; a table named '*' remains literal.
    """

    table: str
    filter_: RowFilterEntryFilter
    table_target: CatalogTableScope | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        filter_ = self.filter_.to_dict()

        table_target: dict[str, Any] | Unset = UNSET
        if not isinstance(self.table_target, Unset):
            table_target = self.table_target.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
                "filter": filter_,
            }
        )
        if table_target is not UNSET:
            field_dict["table_target"] = table_target

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.catalog_table_scope import CatalogTableScope
        from ..models.row_filter_entry_filter import RowFilterEntryFilter

        d = dict(src_dict)
        table = d.pop("table")

        filter_ = RowFilterEntryFilter.from_dict(d.pop("filter"))

        _table_target = d.pop("table_target", UNSET)
        table_target: CatalogTableScope | Unset
        if isinstance(_table_target, Unset):
            table_target = UNSET
        else:
            table_target = CatalogTableScope.from_dict(_table_target)

        row_filter_entry = cls(
            table=table,
            filter_=filter_,
            table_target=table_target,
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
