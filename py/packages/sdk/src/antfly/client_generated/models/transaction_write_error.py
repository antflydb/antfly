from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TransactionWriteError")


@_attrs_define
class TransactionWriteError:
    """A classified write failure. An aborted status confirms that the transaction did not commit.

    Attributes:
        code (str): Machine-readable cause, including row_policy_denied and transaction_precommit_aborted.
        message (str):
        retryable (bool):
        status (str | Unset): Set to aborted for a confirmed transaction abort.
        transaction_id (str | Unset): The transaction ID for session commit responses.
        retry_after_ms (int | Unset):
    """

    code: str
    message: str
    retryable: bool
    status: str | Unset = UNSET
    transaction_id: str | Unset = UNSET
    retry_after_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        code = self.code

        message = self.message

        retryable = self.retryable

        status = self.status

        transaction_id = self.transaction_id

        retry_after_ms = self.retry_after_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "code": code,
                "message": message,
                "retryable": retryable,
            }
        )
        if status is not UNSET:
            field_dict["status"] = status
        if transaction_id is not UNSET:
            field_dict["transaction_id"] = transaction_id
        if retry_after_ms is not UNSET:
            field_dict["retry_after_ms"] = retry_after_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = d.pop("code")

        message = d.pop("message")

        retryable = d.pop("retryable")

        status = d.pop("status", UNSET)

        transaction_id = d.pop("transaction_id", UNSET)

        retry_after_ms = d.pop("retry_after_ms", UNSET)

        transaction_write_error = cls(
            code=code,
            message=message,
            retryable=retryable,
            status=status,
            transaction_id=transaction_id,
            retry_after_ms=retry_after_ms,
        )

        transaction_write_error.additional_properties = d
        return transaction_write_error

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
