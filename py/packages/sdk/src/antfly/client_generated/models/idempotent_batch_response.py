from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.idempotent_batch_response_status import IdempotentBatchResponseStatus

T = TypeVar("T", bound="IdempotentBatchResponse")


@_attrs_define
class IdempotentBatchResponse:
    """
    Attributes:
        status (IdempotentBatchResponseStatus): Durable commit and recovery state for this operation.
        inserted (int): Number of documents inserted by the sealed operation.
        deleted (int): Number of documents deleted by the sealed operation.
        transformed (int): Number of documents transformed by the sealed operation.
        transaction_id (str): Stable transaction receipt ID for replay and reconciliation.
        reconcile (str): Transaction-session status path for this operation.
    """

    status: IdempotentBatchResponseStatus
    inserted: int
    deleted: int
    transformed: int
    transaction_id: str
    reconcile: str

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        inserted = self.inserted

        deleted = self.deleted

        transformed = self.transformed

        transaction_id = self.transaction_id

        reconcile = self.reconcile

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "inserted": inserted,
                "deleted": deleted,
                "transformed": transformed,
                "transaction_id": transaction_id,
                "reconcile": reconcile,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = IdempotentBatchResponseStatus(d.pop("status"))

        inserted = d.pop("inserted")

        deleted = d.pop("deleted")

        transformed = d.pop("transformed")

        transaction_id = d.pop("transaction_id")

        reconcile = d.pop("reconcile")

        idempotent_batch_response = cls(
            status=status,
            inserted=inserted,
            deleted=deleted,
            transformed=transformed,
            transaction_id=transaction_id,
            reconcile=reconcile,
        )

        return idempotent_batch_response
