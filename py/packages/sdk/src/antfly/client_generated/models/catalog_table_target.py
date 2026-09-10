from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="CatalogTableTarget")


@_attrs_define
class CatalogTableTarget:
    """An explicit native table target. Components are literal names; dots do not qualify a string table name.

    Attributes:
        table (str):
        database (str | Unset):  Default: 'default'.
        namespace (str | Unset):  Default: 'public'.
    """

    table: str
    database: str | Unset = "default"
    namespace: str | Unset = "public"

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        database = self.database

        namespace = self.namespace

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "table": table,
            }
        )
        if database is not UNSET:
            field_dict["database"] = database
        if namespace is not UNSET:
            field_dict["namespace"] = namespace

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table")

        database = d.pop("database", UNSET)

        namespace = d.pop("namespace", UNSET)

        catalog_table_target = cls(
            table=table,
            database=database,
            namespace=namespace,
        )

        return catalog_table_target
