from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLPrepareRequest")


@_attrs_define
class SQLPrepareRequest:
    """
    Attributes:
        statement (str):
        database (str | Unset):
        namespace (str | Unset):
    """

    statement: str
    database: str | Unset = UNSET
    namespace: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        statement = self.statement

        database = self.database

        namespace = self.namespace

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "statement": statement,
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
        statement = d.pop("statement")

        database = d.pop("database", UNSET)

        namespace = d.pop("namespace", UNSET)

        sql_prepare_request = cls(
            statement=statement,
            database=database,
            namespace=namespace,
        )

        return sql_prepare_request
