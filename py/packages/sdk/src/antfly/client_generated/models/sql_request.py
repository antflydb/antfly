from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.sql_request_lake_visibility import SQLRequestLakeVisibility
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.read_requirements import ReadRequirements


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
            read_requirements (ReadRequirements | Unset): Freshness requirements for writable Iceberg reads. Search supports
                accepted and published (default accepted); SQL supports accepted and committed (default committed). Unsupported
                visibility modes fail explicitly. Cannot be combined with legacy lake_read or lake_visibility. A pinned
                transaction snapshot never advances to satisfy a newer receipt.
            lake_visibility (SQLRequestLakeVisibility | Unset): Read visibility for writable Iceberg sources. Accepted pins
                typed WAL changes over the committed snapshot; unavailable coverage fails closed. HTTP sessions and prepared
                executions inside a transaction retain the first accepted read cut for each table, including across savepoint
                rollback. Cuts expire after one hour. Serializable write transactions over external sources are not supported.
                Prefer read_requirements.visibility; this field is a deprecated alias.
            parameters (list[Any] | Unset): Positional JSON parameter values, including null.
            database (str | Unset): Database used to resolve unqualified catalog names.
            namespace (str | Unset): Namespace used to resolve unqualified table names.
            limit (int | Unset): Maximum admitted result rows; does not change statement semantics. Default: 128.
            session_id (str | Unset): Opaque SQL session identifier returned by a previous response.
            connection_id (str | Unset): Durable idle HTTP connection. Its scope and settings are inherited; active
                transaction IDs cannot be bypassed by omitting session_id. DISCARD ALL requires this identity and is rejected
                while a transaction is active or uncertain.
    """

    statement: str
    read_requirements: ReadRequirements | Unset = UNSET
    lake_visibility: SQLRequestLakeVisibility | Unset = UNSET
    parameters: list[Any] | Unset = UNSET
    database: str | Unset = UNSET
    namespace: str | Unset = UNSET
    limit: int | Unset = 128
    session_id: str | Unset = UNSET
    connection_id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        statement = self.statement

        read_requirements: dict[str, Any] | Unset = UNSET
        if not isinstance(self.read_requirements, Unset):
            read_requirements = self.read_requirements.to_dict()

        lake_visibility: str | Unset = UNSET
        if not isinstance(self.lake_visibility, Unset):
            lake_visibility = self.lake_visibility.value

        parameters: list[Any] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = self.parameters

        database = self.database

        namespace = self.namespace

        limit = self.limit

        session_id = self.session_id

        connection_id = self.connection_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "statement": statement,
            }
        )
        if read_requirements is not UNSET:
            field_dict["read_requirements"] = read_requirements
        if lake_visibility is not UNSET:
            field_dict["lake_visibility"] = lake_visibility
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
        if connection_id is not UNSET:
            field_dict["connection_id"] = connection_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.read_requirements import ReadRequirements

        d = dict(src_dict)
        statement = d.pop("statement")

        _read_requirements = d.pop("read_requirements", UNSET)
        read_requirements: ReadRequirements | Unset
        if isinstance(_read_requirements, Unset):
            read_requirements = UNSET
        else:
            read_requirements = ReadRequirements.from_dict(_read_requirements)

        _lake_visibility = d.pop("lake_visibility", UNSET)
        lake_visibility: SQLRequestLakeVisibility | Unset
        if isinstance(_lake_visibility, Unset):
            lake_visibility = UNSET
        else:
            lake_visibility = SQLRequestLakeVisibility(_lake_visibility)

        parameters = cast(list[Any], d.pop("parameters", UNSET))

        database = d.pop("database", UNSET)

        namespace = d.pop("namespace", UNSET)

        limit = d.pop("limit", UNSET)

        session_id = d.pop("session_id", UNSET)

        connection_id = d.pop("connection_id", UNSET)

        sql_request = cls(
            statement=statement,
            read_requirements=read_requirements,
            lake_visibility=lake_visibility,
            parameters=parameters,
            database=database,
            namespace=namespace,
            limit=limit,
            session_id=session_id,
            connection_id=connection_id,
        )

        return sql_request
