from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="CatalogTableScope")


@_attrs_define
class CatalogTableScope:
    """A table or all tables in an explicit namespace. A missing table selects the namespace; a table named '*' remains
    literal.

        Attributes:
            database (str | Unset):  Default: 'default'.
            namespace (str | Unset):  Default: 'public'.
            table (str | Unset):
    """

    database: str | Unset = "default"
    namespace: str | Unset = "public"
    table: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        database = self.database

        namespace = self.namespace

        table = self.table

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if database is not UNSET:
            field_dict["database"] = database
        if namespace is not UNSET:
            field_dict["namespace"] = namespace
        if table is not UNSET:
            field_dict["table"] = table

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        database = d.pop("database", UNSET)

        namespace = d.pop("namespace", UNSET)

        table = d.pop("table", UNSET)

        catalog_table_scope = cls(
            database=database,
            namespace=namespace,
            table=table,
        )

        return catalog_table_scope
