from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.sql_statement_response_kind import SqlStatementResponseKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_statement_response_applied import SqlStatementResponseApplied
    from ..models.sql_statement_response_result_type_0 import SqlStatementResponseResultType0


T = TypeVar("T", bound="SqlStatementResponse")


@_attrs_define
class SqlStatementResponse:
    """Synchronous SQL statement result metadata. Catalog/session/control
    statements route through the typed DDL/session execution path. Read
    statements lower through the same typed row-plan executor used by the
    JSON relational rows APIs. Point write statements lower through the
    typed row-batch mutation path, and insert-from-source statements lower
    through the typed row-read plus row-batch mutation path.

        Attributes:
            kind (SqlStatementResponseKind):
            session_id (int):
            noop (bool | Unset):
            applied (SqlStatementResponseApplied | Unset): Applied DDL/session result record.
            statement_kind (None | str | Unset): Lowered read or write statement family for data responses.
            result (None | SqlStatementResponseResultType0 | Unset): Typed relational row-plan, row-batch, or mutation-
                source response for data statements.
    """

    kind: SqlStatementResponseKind
    session_id: int
    noop: bool | Unset = UNSET
    applied: SqlStatementResponseApplied | Unset = UNSET
    statement_kind: None | str | Unset = UNSET
    result: None | SqlStatementResponseResultType0 | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.sql_statement_response_result_type_0 import SqlStatementResponseResultType0

        kind = self.kind.value

        session_id = self.session_id

        noop = self.noop

        applied: dict[str, Any] | Unset = UNSET
        if not isinstance(self.applied, Unset):
            applied = self.applied.to_dict()

        statement_kind: None | str | Unset
        if isinstance(self.statement_kind, Unset):
            statement_kind = UNSET
        else:
            statement_kind = self.statement_kind

        result: dict[str, Any] | None | Unset
        if isinstance(self.result, Unset):
            result = UNSET
        elif isinstance(self.result, SqlStatementResponseResultType0):
            result = self.result.to_dict()
        else:
            result = self.result

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "session_id": session_id,
            }
        )
        if noop is not UNSET:
            field_dict["noop"] = noop
        if applied is not UNSET:
            field_dict["applied"] = applied
        if statement_kind is not UNSET:
            field_dict["statement_kind"] = statement_kind
        if result is not UNSET:
            field_dict["result"] = result

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_statement_response_applied import SqlStatementResponseApplied
        from ..models.sql_statement_response_result_type_0 import SqlStatementResponseResultType0

        d = dict(src_dict)
        kind = SqlStatementResponseKind(d.pop("kind"))

        session_id = d.pop("session_id")

        noop = d.pop("noop", UNSET)

        _applied = d.pop("applied", UNSET)
        applied: SqlStatementResponseApplied | Unset
        if isinstance(_applied, Unset):
            applied = UNSET
        else:
            applied = SqlStatementResponseApplied.from_dict(_applied)

        def _parse_statement_kind(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        statement_kind = _parse_statement_kind(d.pop("statement_kind", UNSET))

        def _parse_result(data: object) -> None | SqlStatementResponseResultType0 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                result_type_0 = SqlStatementResponseResultType0.from_dict(data)

                return result_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | SqlStatementResponseResultType0 | Unset, data)

        result = _parse_result(d.pop("result", UNSET))

        sql_statement_response = cls(
            kind=kind,
            session_id=session_id,
            noop=noop,
            applied=applied,
            statement_kind=statement_kind,
            result=result,
        )

        return sql_statement_response
