from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="EnrichmentContextSql")


@_attrs_define
class EnrichmentContextSql:
    """Ranking clause: a read-only SQL `SELECT` (see `POST /sql`), run as the principal that declared the enrichment (its
    table permissions and row policies apply; the server binds the declaration to that principal, and revoking the
    credential stops rebuilds). Members are the rows of the source `table` named by the result's `_id` column (every
    table exposes its document key as `_id`; alias another key column `AS _id`), in result order, each once, up to
    `limit`, and render as those rows' `fields`. A result without an `_id` column fails. Results are admitted up to 4096
    rows; a larger result fails rather than truncating, so bound it with `LIMIT`. Only `limit` may sit beside it. A SQL
    source is a snapshot: dependents rebuild on `refresh_interval` or manually. Any statement but a `SELECT` is
    rejected. Embedded Lite resolves names in its single catalog and rejects `database` and `namespace`.

        Attributes:
            statement (str): One SQL SELECT statement.
            parameters (list[Any] | Unset): Positional parameters (`$1`, `$2`, ...); values are literals or
                `{"$source":"/pointer"}` bindings from the dependent row.
            database (str | Unset): Database used to resolve unqualified names (default `default`).
            namespace (str | Unset): Namespace used to resolve unqualified table names (default `public`).
    """

    statement: str
    parameters: list[Any] | Unset = UNSET
    database: str | Unset = UNSET
    namespace: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        statement = self.statement

        parameters: list[Any] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = self.parameters

        database = self.database

        namespace = self.namespace

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

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        statement = d.pop("statement")

        parameters = cast(list[Any], d.pop("parameters", UNSET))

        database = d.pop("database", UNSET)

        namespace = d.pop("namespace", UNSET)

        enrichment_context_sql = cls(
            statement=statement,
            parameters=parameters,
            database=database,
            namespace=namespace,
        )

        return enrichment_context_sql
