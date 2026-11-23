from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_transaction_status import SQLTransactionStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_notice import SQLNotice


T = TypeVar("T", bound="SQLDiagnostic")


@_attrs_define
class SQLDiagnostic:
    """
    Attributes:
        code (str): Five-character SQLSTATE error code.
        message (str): Human-readable diagnostic with no sensitive parameter values.
        notices (list[SQLNotice] | Unset):
        column_name (str | Unset): Public column identifier for a column constraint violation; no row values are
            included.
        position (int | Unset): Optional one-based character position in the submitted SQL statement.
        transaction_id (str | Unset): Native transaction receipt for reconciliation when a mutation outcome is unknown.
        retryable (bool | Unset): False for SQLSTATE 40003; never replay a mutation whose outcome is unknown.
        transaction_status (SQLTransactionStatus | Unset): Authoritative native SQL session state after the statement.
            Failed sessions require ROLLBACK or ROLLBACK TO SAVEPOINT; uncertain commit outcomes must be reconciled by
            transaction_id, never replayed.
    """

    code: str
    message: str
    notices: list[SQLNotice] | Unset = UNSET
    column_name: str | Unset = UNSET
    position: int | Unset = UNSET
    transaction_id: str | Unset = UNSET
    retryable: bool | Unset = UNSET
    transaction_status: SQLTransactionStatus | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        code = self.code

        message = self.message

        notices: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.notices, Unset):
            notices = []
            for notices_item_data in self.notices:
                notices_item = notices_item_data.to_dict()
                notices.append(notices_item)

        column_name = self.column_name

        position = self.position

        transaction_id = self.transaction_id

        retryable = self.retryable

        transaction_status: str | Unset = UNSET
        if not isinstance(self.transaction_status, Unset):
            transaction_status = self.transaction_status.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "code": code,
                "message": message,
            }
        )
        if notices is not UNSET:
            field_dict["notices"] = notices
        if column_name is not UNSET:
            field_dict["column_name"] = column_name
        if position is not UNSET:
            field_dict["position"] = position
        if transaction_id is not UNSET:
            field_dict["transaction_id"] = transaction_id
        if retryable is not UNSET:
            field_dict["retryable"] = retryable
        if transaction_status is not UNSET:
            field_dict["transaction_status"] = transaction_status

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_notice import SQLNotice

        d = dict(src_dict)
        code = d.pop("code")

        message = d.pop("message")

        _notices = d.pop("notices", UNSET)
        notices: list[SQLNotice] | Unset = UNSET
        if _notices is not UNSET:
            notices = []
            for notices_item_data in _notices:
                notices_item = SQLNotice.from_dict(notices_item_data)

                notices.append(notices_item)

        column_name = d.pop("column_name", UNSET)

        position = d.pop("position", UNSET)

        transaction_id = d.pop("transaction_id", UNSET)

        retryable = d.pop("retryable", UNSET)

        _transaction_status = d.pop("transaction_status", UNSET)
        transaction_status: SQLTransactionStatus | Unset
        if isinstance(_transaction_status, Unset):
            transaction_status = UNSET
        else:
            transaction_status = SQLTransactionStatus(_transaction_status)

        sql_diagnostic = cls(
            code=code,
            message=message,
            notices=notices,
            column_name=column_name,
            position=position,
            transaction_id=transaction_id,
            retryable=retryable,
            transaction_status=transaction_status,
        )

        return sql_diagnostic
