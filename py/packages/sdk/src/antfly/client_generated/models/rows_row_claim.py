from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_row_claim_mode import RowsRowClaimMode
from ..models.rows_row_claim_wait_policy import RowsRowClaimWaitPolicy
from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsRowClaim")


@_attrs_define
class RowsRowClaim:
    """Lockable base-row claim metadata. Public row-plan endpoints reject this
    field; it is only accepted by `rows/mutation-source` lockable base-row
    sources and internal/coordinator execution paths. `transaction_id` is
    the canonical field name.

        Attributes:
            mode (RowsRowClaimMode | Unset):  Default: RowsRowClaimMode.FOR_UPDATE.
            wait_policy (RowsRowClaimWaitPolicy | Unset):  Default: RowsRowClaimWaitPolicy.WAIT.
            skip_locked (bool | Unset):  Default: False.
            lease_ms (int | Unset):  Default: 30000.
            owner_id (str | Unset):
            transaction_id (str | Unset): Canonical 16-byte transaction id encoded as 32 hex characters.
    """

    mode: RowsRowClaimMode | Unset = RowsRowClaimMode.FOR_UPDATE
    wait_policy: RowsRowClaimWaitPolicy | Unset = RowsRowClaimWaitPolicy.WAIT
    skip_locked: bool | Unset = False
    lease_ms: int | Unset = 30000
    owner_id: str | Unset = UNSET
    transaction_id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        wait_policy: str | Unset = UNSET
        if not isinstance(self.wait_policy, Unset):
            wait_policy = self.wait_policy.value

        skip_locked = self.skip_locked

        lease_ms = self.lease_ms

        owner_id = self.owner_id

        transaction_id = self.transaction_id

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if mode is not UNSET:
            field_dict["mode"] = mode
        if wait_policy is not UNSET:
            field_dict["wait_policy"] = wait_policy
        if skip_locked is not UNSET:
            field_dict["skip_locked"] = skip_locked
        if lease_ms is not UNSET:
            field_dict["lease_ms"] = lease_ms
        if owner_id is not UNSET:
            field_dict["owner_id"] = owner_id
        if transaction_id is not UNSET:
            field_dict["transaction_id"] = transaction_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _mode = d.pop("mode", UNSET)
        mode: RowsRowClaimMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = RowsRowClaimMode(_mode)

        _wait_policy = d.pop("wait_policy", UNSET)
        wait_policy: RowsRowClaimWaitPolicy | Unset
        if isinstance(_wait_policy, Unset):
            wait_policy = UNSET
        else:
            wait_policy = RowsRowClaimWaitPolicy(_wait_policy)

        skip_locked = d.pop("skip_locked", UNSET)

        lease_ms = d.pop("lease_ms", UNSET)

        owner_id = d.pop("owner_id", UNSET)

        transaction_id = d.pop("transaction_id", UNSET)

        rows_row_claim = cls(
            mode=mode,
            wait_policy=wait_policy,
            skip_locked=skip_locked,
            lease_ms=lease_ms,
            owner_id=owner_id,
            transaction_id=transaction_id,
        )

        return rows_row_claim
