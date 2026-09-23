from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.sql_mutation_outcome import SQLMutationOutcome
from ..models.sql_transaction_status import SQLTransactionStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_column import SQLColumn
    from ..models.sqlddl_receipt import SQLDDLReceipt


T = TypeVar("T", bound="SQLResponse")


@_attrs_define
class SQLResponse:
    """Ordinal result rows with corresponding logical column metadata. SQL NULL
    is JSON null; sql_nulls distinguishes it from a JSON column containing
    the JSON literal null. Integer-typed values are exact decimal strings; datetime
    values are strings. Objects and arrays in JSON columns remain JSON.

        Attributes:
            columns (list[SQLColumn]):
            rows (list[list[Any]]):
            rows_affected (int): Number of rows affected by a mutation, or zero for a read-only statement.
            command_tag (str): SQL command completion tag.
            sql_nulls (list[list[bool]] | Unset): Null flags aligned exactly with rows and their columns. True denotes
                SQL NULL; false denotes a value, including the JSON literal null.
                When omitted, null cells have the legacy SQL NULL interpretation.
            mutation_outcome (SQLMutationOutcome | Unset): Durable mutation outcome. Every value confirms a commit and must
                not
                cause the statement to be replayed. Pending or repair outcomes require
                visibility convergence or operator action rather than another write.
            ddl_receipt (SQLDDLReceipt | Unset): Durable DDL declaration receipt. admission_unknown means admission has
                not been confirmed; reconcile restore_job_id without replaying DDL.
                Pending or invalid means the declaration
                committed but validation has not established an active constraint. Do not
                replay it. Inspect table constraint status using this immutable table
                identity and schema generation; a later generation supersedes this receipt.
            transaction_id (str | Unset): Native transaction receipt for visibility or repair reconciliation; never replay a
                committed statement.
            session_id (str | Unset): Opaque SQL session identifier for subsequent requests.
            transaction_status (SQLTransactionStatus | Unset): Authoritative native SQL session state after the statement.
                Failed sessions require ROLLBACK or ROLLBACK TO SAVEPOINT; uncertain commit outcomes must be reconciled by
                transaction_id, never replayed.
    """

    columns: list[SQLColumn]
    rows: list[list[Any]]
    rows_affected: int
    command_tag: str
    sql_nulls: list[list[bool]] | Unset = UNSET
    mutation_outcome: SQLMutationOutcome | Unset = UNSET
    ddl_receipt: SQLDDLReceipt | Unset = UNSET
    transaction_id: str | Unset = UNSET
    session_id: str | Unset = UNSET
    transaction_status: SQLTransactionStatus | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        columns = []
        for columns_item_data in self.columns:
            columns_item = columns_item_data.to_dict()
            columns.append(columns_item)

        rows = []
        for rows_item_data in self.rows:
            rows_item = rows_item_data

            rows.append(rows_item)

        rows_affected = self.rows_affected

        command_tag = self.command_tag

        sql_nulls: list[list[bool]] | Unset = UNSET
        if not isinstance(self.sql_nulls, Unset):
            sql_nulls = []
            for sql_nulls_item_data in self.sql_nulls:
                sql_nulls_item = sql_nulls_item_data

                sql_nulls.append(sql_nulls_item)

        mutation_outcome: str | Unset = UNSET
        if not isinstance(self.mutation_outcome, Unset):
            mutation_outcome = self.mutation_outcome.value

        ddl_receipt: dict[str, Any] | Unset = UNSET
        if not isinstance(self.ddl_receipt, Unset):
            ddl_receipt = self.ddl_receipt.to_dict()

        transaction_id = self.transaction_id

        session_id = self.session_id

        transaction_status: str | Unset = UNSET
        if not isinstance(self.transaction_status, Unset):
            transaction_status = self.transaction_status.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "columns": columns,
                "rows": rows,
                "rows_affected": rows_affected,
                "command_tag": command_tag,
            }
        )
        if sql_nulls is not UNSET:
            field_dict["sql_nulls"] = sql_nulls
        if mutation_outcome is not UNSET:
            field_dict["mutation_outcome"] = mutation_outcome
        if ddl_receipt is not UNSET:
            field_dict["ddl_receipt"] = ddl_receipt
        if transaction_id is not UNSET:
            field_dict["transaction_id"] = transaction_id
        if session_id is not UNSET:
            field_dict["session_id"] = session_id
        if transaction_status is not UNSET:
            field_dict["transaction_status"] = transaction_status

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_column import SQLColumn
        from ..models.sqlddl_receipt import SQLDDLReceipt

        d = dict(src_dict)
        columns = []
        _columns = d.pop("columns")
        for columns_item_data in _columns:
            columns_item = SQLColumn.from_dict(columns_item_data)

            columns.append(columns_item)

        rows = []
        _rows = d.pop("rows")
        for rows_item_data in _rows:
            rows_item = cast(list[Any], rows_item_data)

            rows.append(rows_item)

        rows_affected = d.pop("rows_affected")

        command_tag = d.pop("command_tag")

        _sql_nulls = d.pop("sql_nulls", UNSET)
        sql_nulls: list[list[bool]] | Unset = UNSET
        if _sql_nulls is not UNSET:
            sql_nulls = []
            for sql_nulls_item_data in _sql_nulls:
                sql_nulls_item = cast(list[bool], sql_nulls_item_data)

                sql_nulls.append(sql_nulls_item)

        _mutation_outcome = d.pop("mutation_outcome", UNSET)
        mutation_outcome: SQLMutationOutcome | Unset
        if isinstance(_mutation_outcome, Unset):
            mutation_outcome = UNSET
        else:
            mutation_outcome = SQLMutationOutcome(_mutation_outcome)

        _ddl_receipt = d.pop("ddl_receipt", UNSET)
        ddl_receipt: SQLDDLReceipt | Unset
        if isinstance(_ddl_receipt, Unset):
            ddl_receipt = UNSET
        else:
            ddl_receipt = SQLDDLReceipt.from_dict(_ddl_receipt)

        transaction_id = d.pop("transaction_id", UNSET)

        session_id = d.pop("session_id", UNSET)

        _transaction_status = d.pop("transaction_status", UNSET)
        transaction_status: SQLTransactionStatus | Unset
        if isinstance(_transaction_status, Unset):
            transaction_status = UNSET
        else:
            transaction_status = SQLTransactionStatus(_transaction_status)

        sql_response = cls(
            columns=columns,
            rows=rows,
            rows_affected=rows_affected,
            command_tag=command_tag,
            sql_nulls=sql_nulls,
            mutation_outcome=mutation_outcome,
            ddl_receipt=ddl_receipt,
            transaction_id=transaction_id,
            session_id=session_id,
            transaction_status=transaction_status,
        )

        return sql_response
