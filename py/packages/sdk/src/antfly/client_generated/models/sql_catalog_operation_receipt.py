from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_catalog_object_kind import SQLCatalogObjectKind
from ..models.sql_catalog_operation_kind import SQLCatalogOperationKind
from ..models.sql_catalog_operation_state import SQLCatalogOperationState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_diagnostic import SQLDiagnostic


T = TypeVar("T", bound="SQLCatalogOperationReceipt")


@_attrs_define
class SQLCatalogOperationReceipt:
    """Recovery receipt for an atomic native catalog mutation, separate from
    physical table/schema publication receipts. Retain recovery_token and
    observe it with getSQLCatalogOperation; never resubmit uncertain DDL.
    Retention is bounded by catalog activity, not a promised wall-clock
    interval. Missing, expired, inaccessible or superseded-authority outcomes
    cannot be inferred from a current object name and remain unknown.
    This receipt does not make a new request idempotent or recover a token
    lost with an entire response. All identity integers are exact decimal strings.

        Attributes:
            kind (SQLCatalogObjectKind): Native catalog resource kind; never a synthetic physical table.
            operation (SQLCatalogOperationKind):
            database (str):
            namespace (str):
            name (str):
            state (SQLCatalogOperationState): A retained authoritative result, or an unconfirmed outcome. Unknown is never
                permission to replay DDL.
            recovery_token (str): Opaque versioned operation handle. Send it unchanged; it is not a reusable admission
                ticket.
            object_id (str | Unset): Actual committed catalog identity, when the result carries one; never a predicted
                allocation.
            generation (str | Unset):
            catalog_revision (str | Unset):
            diagnostic (SQLDiagnostic | Unset):
    """

    kind: SQLCatalogObjectKind
    operation: SQLCatalogOperationKind
    database: str
    namespace: str
    name: str
    state: SQLCatalogOperationState
    recovery_token: str
    object_id: str | Unset = UNSET
    generation: str | Unset = UNSET
    catalog_revision: str | Unset = UNSET
    diagnostic: SQLDiagnostic | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        operation = self.operation.value

        database = self.database

        namespace = self.namespace

        name = self.name

        state = self.state.value

        recovery_token = self.recovery_token

        object_id = self.object_id

        generation = self.generation

        catalog_revision = self.catalog_revision

        diagnostic: dict[str, Any] | Unset = UNSET
        if not isinstance(self.diagnostic, Unset):
            diagnostic = self.diagnostic.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "operation": operation,
                "database": database,
                "namespace": namespace,
                "name": name,
                "state": state,
                "recovery_token": recovery_token,
            }
        )
        if object_id is not UNSET:
            field_dict["object_id"] = object_id
        if generation is not UNSET:
            field_dict["generation"] = generation
        if catalog_revision is not UNSET:
            field_dict["catalog_revision"] = catalog_revision
        if diagnostic is not UNSET:
            field_dict["diagnostic"] = diagnostic

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_diagnostic import SQLDiagnostic

        d = dict(src_dict)
        kind = SQLCatalogObjectKind(d.pop("kind"))

        operation = SQLCatalogOperationKind(d.pop("operation"))

        database = d.pop("database")

        namespace = d.pop("namespace")

        name = d.pop("name")

        state = SQLCatalogOperationState(d.pop("state"))

        recovery_token = d.pop("recovery_token")

        object_id = d.pop("object_id", UNSET)

        generation = d.pop("generation", UNSET)

        catalog_revision = d.pop("catalog_revision", UNSET)

        _diagnostic = d.pop("diagnostic", UNSET)
        diagnostic: SQLDiagnostic | Unset
        if isinstance(_diagnostic, Unset):
            diagnostic = UNSET
        else:
            diagnostic = SQLDiagnostic.from_dict(_diagnostic)

        sql_catalog_operation_receipt = cls(
            kind=kind,
            operation=operation,
            database=database,
            namespace=namespace,
            name=name,
            state=state,
            recovery_token=recovery_token,
            object_id=object_id,
            generation=generation,
            catalog_revision=catalog_revision,
            diagnostic=diagnostic,
        )

        return sql_catalog_operation_receipt
