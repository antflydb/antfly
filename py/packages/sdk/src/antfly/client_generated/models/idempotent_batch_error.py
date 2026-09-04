from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.idempotent_batch_error_status import IdempotentBatchErrorStatus

T = TypeVar("T", bound="IdempotentBatchError")


@_attrs_define
class IdempotentBatchError:
    """
    Attributes:
        status (IdempotentBatchErrorStatus):
        code (str):
        message (str):
        retryable (bool):
        transaction_id (str):
        reconcile (str): Transaction-session status path for this keyed batch.
    """

    status: IdempotentBatchErrorStatus
    code: str
    message: str
    retryable: bool
    transaction_id: str
    reconcile: str

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        code = self.code

        message = self.message

        retryable = self.retryable

        transaction_id = self.transaction_id

        reconcile = self.reconcile

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "code": code,
                "message": message,
                "retryable": retryable,
                "transaction_id": transaction_id,
                "reconcile": reconcile,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = IdempotentBatchErrorStatus(d.pop("status"))

        code = d.pop("code")

        message = d.pop("message")

        retryable = d.pop("retryable")

        transaction_id = d.pop("transaction_id")

        reconcile = d.pop("reconcile")

        idempotent_batch_error = cls(
            status=status,
            code=code,
            message=message,
            retryable=retryable,
            transaction_id=transaction_id,
            reconcile=reconcile,
        )

        return idempotent_batch_error
