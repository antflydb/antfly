from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLRequest")


@_attrs_define
class SQLRequest:
    """Execute one SQL statement. Parameters are positional (`$1`, `$2`, ...),
    never interpolated into SQL text. To preserve integer precision in
    JavaScript clients, supply integers outside the exact JSON number range
    as decimal strings; binding coerces parameters to the expected type.
    The result limit is an admission bound, not an implicit SQL LIMIT:
    statements whose results exceed it fail instead of silently truncating.
    Request bodies are limited to 4 MiB, preparation to 8 MiB of allocated
    memory, and encoded results to a 16 MiB allocation budget.

        Attributes:
            statement (str): A single SQL statement.
            parameters (list[Any] | Unset): Positional JSON parameter values, including null.
            database (str | Unset): Database used to resolve unqualified catalog names.
            namespace (str | Unset): Namespace used to resolve unqualified table names.
            limit (int | Unset): Maximum admitted result rows; does not change statement semantics. Default: 128.
            session_id (str | Unset): Opaque SQL session identifier returned by a previous response.
    """

    statement: str
    parameters: list[Any] | Unset = UNSET
    database: str | Unset = UNSET
    namespace: str | Unset = UNSET
    limit: int | Unset = 128
    session_id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        statement = self.statement

        parameters: list[Any] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = self.parameters

        database = self.database

        namespace = self.namespace

        limit = self.limit

        session_id = self.session_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "statement": statement,
            }
        )
        if parameters is not UNSET:
            field_dict["parameters"] = parameters
        if database is not UNSET:
            field_dict["database"] = database
        if namespace is not UNSET:
            field_dict["namespace"] = namespace
        if limit is not UNSET:
            field_dict["limit"] = limit
        if session_id is not UNSET:
            field_dict["session_id"] = session_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        statement = d.pop("statement")

        parameters = cast(list[Any], d.pop("parameters", UNSET))

        database = d.pop("database", UNSET)

        namespace = d.pop("namespace", UNSET)

        limit = d.pop("limit", UNSET)

        session_id = d.pop("session_id", UNSET)

        sql_request = cls(
            statement=statement,
            parameters=parameters,
            database=database,
            namespace=namespace,
            limit=limit,
            session_id=session_id,
        )

        return sql_request
