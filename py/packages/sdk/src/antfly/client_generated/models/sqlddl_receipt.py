from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.sqlddl_receipt_state import SQLDDLReceiptState
from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLDDLReceipt")


@_attrs_define
class SQLDDLReceipt:
    """Durable DDL declaration receipt. admission_unknown means admission has
    not been confirmed; reconcile restore_job_id and idempotency_key without replaying DDL.
    Pending or invalid means the declaration
    committed but validation has not established an active constraint. Do not
    replay it. Inspect table constraint status using this immutable table
    identity and schema generation; a later generation supersedes this receipt.

        Attributes:
            database (str):
            namespace (str):
            table (str):
            table_id (str):
            schema_version (int):
            state (SQLDDLReceiptState):
            diagnostic (str | Unset):
            restore_job_id (str | Unset): Native staging job for an atomic schema rewrite or TRUNCATE generation barrier.
                table_id identifies the source generation. Poll the job; pending or admission_unknown is not completed DDL and
                must not be replayed.
            idempotency_key (str | Unset): Durable retry identity for an admitted or uncertain schema rewrite. Retain it
                with restore_job_id when reconciling admission; do not replay the DDL.
    """

    database: str
    namespace: str
    table: str
    table_id: str
    schema_version: int
    state: SQLDDLReceiptState
    diagnostic: str | Unset = UNSET
    restore_job_id: str | Unset = UNSET
    idempotency_key: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        database = self.database

        namespace = self.namespace

        table = self.table

        table_id = self.table_id

        schema_version = self.schema_version

        state = self.state.value

        diagnostic = self.diagnostic

        restore_job_id = self.restore_job_id

        idempotency_key = self.idempotency_key

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "database": database,
                "namespace": namespace,
                "table": table,
                "table_id": table_id,
                "schema_version": schema_version,
                "state": state,
            }
        )
        if diagnostic is not UNSET:
            field_dict["diagnostic"] = diagnostic
        if restore_job_id is not UNSET:
            field_dict["restore_job_id"] = restore_job_id
        if idempotency_key is not UNSET:
            field_dict["idempotency_key"] = idempotency_key

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        database = d.pop("database")

        namespace = d.pop("namespace")

        table = d.pop("table")

        table_id = d.pop("table_id")

        schema_version = d.pop("schema_version")

        state = SQLDDLReceiptState(d.pop("state"))

        diagnostic = d.pop("diagnostic", UNSET)

        restore_job_id = d.pop("restore_job_id", UNSET)

        idempotency_key = d.pop("idempotency_key", UNSET)

        sqlddl_receipt = cls(
            database=database,
            namespace=namespace,
            table=table,
            table_id=table_id,
            schema_version=schema_version,
            state=state,
            diagnostic=diagnostic,
            restore_job_id=restore_job_id,
            idempotency_key=idempotency_key,
        )

        return sqlddl_receipt
