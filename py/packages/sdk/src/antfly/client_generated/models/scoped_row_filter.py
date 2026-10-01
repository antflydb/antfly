from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.catalog_table_scope import CatalogTableScope
    from ..models.scoped_row_filter_filter import ScopedRowFilterFilter


T = TypeVar("T", bound="ScopedRowFilter")


@_attrs_define
class ScopedRowFilter:
    """
    Attributes:
        table_target (CatalogTableScope): A table or all tables in an explicit namespace. A missing table selects the
            namespace; a table named '*' remains literal.
        filter_ (ScopedRowFilterFilter):
    """

    table_target: CatalogTableScope
    filter_: ScopedRowFilterFilter
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table_target = self.table_target.to_dict()

        filter_ = self.filter_.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table_target": table_target,
                "filter": filter_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.catalog_table_scope import CatalogTableScope
        from ..models.scoped_row_filter_filter import ScopedRowFilterFilter

        d = dict(src_dict)
        table_target = CatalogTableScope.from_dict(d.pop("table_target"))

        filter_ = ScopedRowFilterFilter.from_dict(d.pop("filter"))

        scoped_row_filter = cls(
            table_target=table_target,
            filter_=filter_,
        )

        scoped_row_filter.additional_properties = d
        return scoped_row_filter

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
