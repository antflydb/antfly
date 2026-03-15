from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.foreign_source_type import ForeignSourceType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.foreign_column import ForeignColumn


T = TypeVar("T", bound="ForeignSource")


@_attrs_define
class ForeignSource:
    """
    Attributes:
        type_ (ForeignSourceType): Type of the foreign data source. Currently only "postgres" is supported.
             Example: postgres.
        dsn (str): Data source name (connection string) for the foreign database.
            Supports `${secret:key_name}` references that resolve from the Antfly keystore
            or environment variables.
             Example: ${secret:pg_dsn}.
        postgres_table (str): Name of the table or view in the foreign PostgreSQL database to query.
             Example: customers.
        columns (Union[Unset, list['ForeignColumn']]): Optional column definitions for the foreign table. If omitted,
            columns are
            auto-discovered from `information_schema.columns` on first query.
    """

    type_: ForeignSourceType
    dsn: str
    postgres_table: str
    columns: Union[Unset, list["ForeignColumn"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        dsn = self.dsn

        postgres_table = self.postgres_table

        columns: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.columns, Unset):
            columns = []
            for columns_item_data in self.columns:
                columns_item = columns_item_data.to_dict()
                columns.append(columns_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "dsn": dsn,
                "postgres_table": postgres_table,
            }
        )
        if columns is not UNSET:
            field_dict["columns"] = columns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.foreign_column import ForeignColumn

        d = dict(src_dict)
        type_ = ForeignSourceType(d.pop("type"))

        dsn = d.pop("dsn")

        postgres_table = d.pop("postgres_table")

        columns = []
        _columns = d.pop("columns", UNSET)
        for columns_item_data in _columns or []:
            columns_item = ForeignColumn.from_dict(columns_item_data)

            columns.append(columns_item)

        foreign_source = cls(
            type_=type_,
            dsn=dsn,
            postgres_table=postgres_table,
            columns=columns,
        )

        foreign_source.additional_properties = d
        return foreign_source

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
