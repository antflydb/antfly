from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sync_level import SyncLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.row_operation import RowOperation


T = TypeVar("T", bound="RowsBatchRequest")


@_attrs_define
class RowsBatchRequest:
    """
    Attributes:
        operations (list[RowOperation]):
        sync_level (SyncLevel | Unset): Synchronization level for batch operations:
            - "propose": Wait for Raft proposal acceptance (fastest, default)
            - "write": Wait for Pebble KV write
            - "query": Wait until affected documents are visible to query paths such as full-text search
            - "enrichments": Pre-compute enrichments before Raft proposal (synchronous enrichment generation)
            - "full_index": Wait for all index writes to complete (full-text + enrichments + vector indexes)
    """

    operations: list[RowOperation]
    sync_level: SyncLevel | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        operations = []
        for operations_item_data in self.operations:
            operations_item = operations_item_data.to_dict()
            operations.append(operations_item)

        sync_level: str | Unset = UNSET
        if not isinstance(self.sync_level, Unset):
            sync_level = self.sync_level.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "operations": operations,
            }
        )
        if sync_level is not UNSET:
            field_dict["sync_level"] = sync_level

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.row_operation import RowOperation

        d = dict(src_dict)
        operations = []
        _operations = d.pop("operations")
        for operations_item_data in _operations:
            operations_item = RowOperation.from_dict(operations_item_data)

            operations.append(operations_item)

        _sync_level = d.pop("sync_level", UNSET)
        sync_level: SyncLevel | Unset
        if isinstance(_sync_level, Unset):
            sync_level = UNSET
        else:
            sync_level = SyncLevel(_sync_level)

        rows_batch_request = cls(
            operations=operations,
            sync_level=sync_level,
        )

        return rows_batch_request
