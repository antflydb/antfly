from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.multi_batch_response_status import MultiBatchResponseStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.multi_batch_response_tables import MultiBatchResponseTables


T = TypeVar("T", bound="MultiBatchResponse")


@_attrs_define
class MultiBatchResponse:
    """Response for a cross-table batch operation. Contains per-table results.

    Attributes:
        status (MultiBatchResponseStatus | Unset): Durable commit outcome. Pending states mean requested visibility or
            participant propagation is still completing. `committed_repair_required`
            means the primary writes committed, but a terminal enrichment failure needs
            operator repair and will not be retried indefinitely.
        tables (MultiBatchResponseTables | Unset): Per-table batch results
    """

    status: MultiBatchResponseStatus | Unset = UNSET
    tables: MultiBatchResponseTables | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        tables: dict[str, Any] | Unset = UNSET
        if not isinstance(self.tables, Unset):
            tables = self.tables.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if status is not UNSET:
            field_dict["status"] = status
        if tables is not UNSET:
            field_dict["tables"] = tables

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.multi_batch_response_tables import MultiBatchResponseTables

        d = dict(src_dict)
        _status = d.pop("status", UNSET)
        status: MultiBatchResponseStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = MultiBatchResponseStatus(_status)

        _tables = d.pop("tables", UNSET)
        tables: MultiBatchResponseTables | Unset
        if isinstance(_tables, Unset):
            tables = UNSET
        else:
            tables = MultiBatchResponseTables.from_dict(_tables)

        multi_batch_response = cls(
            status=status,
            tables=tables,
        )

        multi_batch_response.additional_properties = d
        return multi_batch_response

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
