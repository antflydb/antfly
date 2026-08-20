from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.batch_response_status import BatchResponseStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="BatchResponse")


@_attrs_define
class BatchResponse:
    """
    Attributes:
        status (BatchResponseStatus | Unset): Durable commit outcome. `committed_pending` means requested visibility or
            participant propagation is still completing. `committed_repair_required`
            means the primary write committed, but a terminal enrichment failure needs
            operator repair and will not be retried indefinitely.
        inserted (int | Unset): Number of documents successfully inserted
        deleted (int | Unset): Number of documents successfully deleted
        transformed (int | Unset): Number of documents successfully transformed
    """

    status: BatchResponseStatus | Unset = UNSET
    inserted: int | Unset = UNSET
    deleted: int | Unset = UNSET
    transformed: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        inserted = self.inserted

        deleted = self.deleted

        transformed = self.transformed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if status is not UNSET:
            field_dict["status"] = status
        if inserted is not UNSET:
            field_dict["inserted"] = inserted
        if deleted is not UNSET:
            field_dict["deleted"] = deleted
        if transformed is not UNSET:
            field_dict["transformed"] = transformed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _status = d.pop("status", UNSET)
        status: BatchResponseStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = BatchResponseStatus(_status)

        inserted = d.pop("inserted", UNSET)

        deleted = d.pop("deleted", UNSET)

        transformed = d.pop("transformed", UNSET)

        batch_response = cls(
            status=status,
            inserted=inserted,
            deleted=deleted,
            transformed=transformed,
        )

        batch_response.additional_properties = d
        return batch_response

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
