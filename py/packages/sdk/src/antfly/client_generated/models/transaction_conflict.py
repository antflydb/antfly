from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.transaction_conflict_kind import TransactionConflictKind
from ..models.transaction_conflict_retry_scope import TransactionConflictRetryScope
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.transaction_conflict_participant import TransactionConflictParticipant


T = TypeVar("T", bound="TransactionConflict")


@_attrs_define
class TransactionConflict:
    """Structured details for an aborted transaction attempt.

    Attributes:
        table (str): Table where the conflict was detected.
        key (str): Document key associated with the conflict, when applicable.
        message (str): Human-readable conflict description.
        kind (TransactionConflictKind): Stable machine-readable conflict classification.
        retryable (bool): Whether retrying the transaction may succeed without changing its writes.
        retry_after_ms (int | Unset): Minimum suggested delay before retrying a retryable conflict.
        retry_scope (TransactionConflictRetryScope | Unset): Component whose state should be refreshed before retrying.
        expected_version (int | Unset): Version required by the transaction predicate.
        current_version (int | Unset): Version observed while validating the transaction predicate.
        participant (TransactionConflictParticipant | Unset): Participant location and 2PC phase where the conflict
            occurred.
    """

    table: str
    key: str
    message: str
    kind: TransactionConflictKind
    retryable: bool
    retry_after_ms: int | Unset = UNSET
    retry_scope: TransactionConflictRetryScope | Unset = UNSET
    expected_version: int | Unset = UNSET
    current_version: int | Unset = UNSET
    participant: TransactionConflictParticipant | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        key = self.key

        message = self.message

        kind = self.kind.value

        retryable = self.retryable

        retry_after_ms = self.retry_after_ms

        retry_scope: str | Unset = UNSET
        if not isinstance(self.retry_scope, Unset):
            retry_scope = self.retry_scope.value

        expected_version = self.expected_version

        current_version = self.current_version

        participant: dict[str, Any] | Unset = UNSET
        if not isinstance(self.participant, Unset):
            participant = self.participant.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
                "key": key,
                "message": message,
                "kind": kind,
                "retryable": retryable,
            }
        )
        if retry_after_ms is not UNSET:
            field_dict["retry_after_ms"] = retry_after_ms
        if retry_scope is not UNSET:
            field_dict["retry_scope"] = retry_scope
        if expected_version is not UNSET:
            field_dict["expected_version"] = expected_version
        if current_version is not UNSET:
            field_dict["current_version"] = current_version
        if participant is not UNSET:
            field_dict["participant"] = participant

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.transaction_conflict_participant import TransactionConflictParticipant

        d = dict(src_dict)
        table = d.pop("table")

        key = d.pop("key")

        message = d.pop("message")

        kind = TransactionConflictKind(d.pop("kind"))

        retryable = d.pop("retryable")

        retry_after_ms = d.pop("retry_after_ms", UNSET)

        _retry_scope = d.pop("retry_scope", UNSET)
        retry_scope: TransactionConflictRetryScope | Unset
        if isinstance(_retry_scope, Unset):
            retry_scope = UNSET
        else:
            retry_scope = TransactionConflictRetryScope(_retry_scope)

        expected_version = d.pop("expected_version", UNSET)

        current_version = d.pop("current_version", UNSET)

        _participant = d.pop("participant", UNSET)
        participant: TransactionConflictParticipant | Unset
        if isinstance(_participant, Unset):
            participant = UNSET
        else:
            participant = TransactionConflictParticipant.from_dict(_participant)

        transaction_conflict = cls(
            table=table,
            key=key,
            message=message,
            kind=kind,
            retryable=retryable,
            retry_after_ms=retry_after_ms,
            retry_scope=retry_scope,
            expected_version=expected_version,
            current_version=current_version,
            participant=participant,
        )

        transaction_conflict.additional_properties = d
        return transaction_conflict

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
