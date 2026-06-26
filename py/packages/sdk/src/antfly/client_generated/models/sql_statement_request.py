from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="SqlStatementRequest")


@_attrs_define
class SqlStatementRequest:
    """Synchronous SQL statement request. `session_id` is optional on the first
    request and should be reused from prior responses when SQL session state
    must persist across requests.

        Attributes:
            sql (str): SQL statement text to execute.
            session_id (int | None | Unset): Logical SQL session id returned by an earlier SQL response.
            database (None | str | Unset): Optional current database override for this request.
            namespace (None | str | Unset): Optional single search-path namespace override for this request.
            read_only (bool | Unset): Execute this statement under a server-enforced PostgreSQL-style read-only transaction
                guard. Default: False.
    """

    sql: str
    session_id: int | None | Unset = UNSET
    database: None | str | Unset = UNSET
    namespace: None | str | Unset = UNSET
    read_only: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        sql = self.sql

        session_id: int | None | Unset
        if isinstance(self.session_id, Unset):
            session_id = UNSET
        else:
            session_id = self.session_id

        database: None | str | Unset
        if isinstance(self.database, Unset):
            database = UNSET
        else:
            database = self.database

        namespace: None | str | Unset
        if isinstance(self.namespace, Unset):
            namespace = UNSET
        else:
            namespace = self.namespace

        read_only = self.read_only

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "sql": sql,
            }
        )
        if session_id is not UNSET:
            field_dict["session_id"] = session_id
        if database is not UNSET:
            field_dict["database"] = database
        if namespace is not UNSET:
            field_dict["namespace"] = namespace
        if read_only is not UNSET:
            field_dict["read_only"] = read_only

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        sql = d.pop("sql")

        def _parse_session_id(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        session_id = _parse_session_id(d.pop("session_id", UNSET))

        def _parse_database(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        database = _parse_database(d.pop("database", UNSET))

        def _parse_namespace(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        namespace = _parse_namespace(d.pop("namespace", UNSET))

        read_only = d.pop("read_only", UNSET)

        sql_statement_request = cls(
            sql=sql,
            session_id=session_id,
            database=database,
            namespace=namespace,
            read_only=read_only,
        )

        return sql_statement_request
