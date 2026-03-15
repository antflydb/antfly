from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sync_level import SyncLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.multi_batch_request_tables import MultiBatchRequestTables


T = TypeVar("T", bound="MultiBatchRequest")


@_attrs_define
class MultiBatchRequest:
    """Cross-table batch operations in a single atomic transaction.

    Groups batch operations by table name. All operations across all tables
    are committed atomically using distributed 2-phase commit (2PC).

    **Atomicity**: Either all operations across all tables succeed, or none do.
    This enables use cases like transferring a record from one table to another,
    or maintaining referential integrity across tables.

        Attributes:
            tables (MultiBatchRequestTables): Map of table names to batch operations for that table.
                Each entry follows the same format as a single-table BatchRequest.
                 Example: {'users': {'inserts': {'user:123': {'name': 'John Doe', 'email': 'john@example.com'}}}, 'orders':
                {'inserts': {'order:456': {'user_id': 'user:123', 'total': 99.99}}}}.
            sync_level (Union[Unset, SyncLevel]): Synchronization level for batch operations:
                - "propose": Wait for Raft proposal acceptance (fastest, default)
                - "write": Wait for Pebble KV write
                - "full_text": Wait for full-text index WAL write
                - "enrichments": Pre-compute enrichments before Raft proposal (synchronous enrichment generation)
                - "aknn": Wait for vector index write with best-effort synchronous embedding (falls back to async on timeout,
                slowest, most durable)
    """

    tables: "MultiBatchRequestTables"
    sync_level: Union[Unset, SyncLevel] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        tables = self.tables.to_dict()

        sync_level: Union[Unset, str] = UNSET
        if not isinstance(self.sync_level, Unset):
            sync_level = self.sync_level.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "tables": tables,
            }
        )
        if sync_level is not UNSET:
            field_dict["sync_level"] = sync_level

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.multi_batch_request_tables import MultiBatchRequestTables

        d = dict(src_dict)
        tables = MultiBatchRequestTables.from_dict(d.pop("tables"))

        _sync_level = d.pop("sync_level", UNSET)
        sync_level: Union[Unset, SyncLevel]
        if isinstance(_sync_level, Unset):
            sync_level = UNSET
        else:
            sync_level = SyncLevel(_sync_level)

        multi_batch_request = cls(
            tables=tables,
            sync_level=sync_level,
        )

        multi_batch_request.additional_properties = d
        return multi_batch_request

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
