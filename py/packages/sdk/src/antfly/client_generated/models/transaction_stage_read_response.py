from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.transaction_stage_read_snapshot import TransactionStageReadSnapshot


T = TypeVar("T", bound="TransactionStageReadResponse")


@_attrs_define
class TransactionStageReadResponse:
    """
    Attributes:
        status (str):
        transaction_id (str):
        snapshot (TransactionStageReadSnapshot):
    """

    status: str
    transaction_id: str
    snapshot: TransactionStageReadSnapshot
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status = self.status

        transaction_id = self.transaction_id

        snapshot = self.snapshot.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "status": status,
                "transaction_id": transaction_id,
                "snapshot": snapshot,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.transaction_stage_read_snapshot import TransactionStageReadSnapshot

        d = dict(src_dict)
        status = d.pop("status")

        transaction_id = d.pop("transaction_id")

        snapshot = TransactionStageReadSnapshot.from_dict(d.pop("snapshot"))

        transaction_stage_read_response = cls(
            status=status,
            transaction_id=transaction_id,
            snapshot=snapshot,
        )

        transaction_stage_read_response.additional_properties = d
        return transaction_stage_read_response

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
