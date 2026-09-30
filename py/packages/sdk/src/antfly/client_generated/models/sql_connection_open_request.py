from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLConnectionOpenRequest")


@_attrs_define
class SQLConnectionOpenRequest:
    """
    Attributes:
        database (str | Unset):  Default: 'default'.
        namespace (str | Unset):  Default: 'public'.
    """

    database: str | Unset = "default"
    namespace: str | Unset = "public"

    def to_dict(self) -> dict[str, Any]:
        database = self.database

        namespace = self.namespace

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if database is not UNSET:
            field_dict["database"] = database
        if namespace is not UNSET:
            field_dict["namespace"] = namespace

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        database = d.pop("database", UNSET)

        namespace = d.pop("namespace", UNSET)

        sql_connection_open_request = cls(
            database=database,
            namespace=namespace,
        )

        return sql_connection_open_request
