from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sync_level import SyncLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.transaction_commit_request_tables import TransactionCommitRequestTables
    from ..models.transaction_read_item import TransactionReadItem


T = TypeVar("T", bound="TransactionCommitRequest")


@_attrs_define
class TransactionCommitRequest:
    """Stateless OCC (Optimistic Concurrency Control) transaction commit request.

    The client reads documents (capturing version tokens from the X-Antfly-Version
    response header on lookups), computes writes locally, then submits everything
    in this single commit request. The server validates that all read versions
    still match before executing writes atomically via 2PC.

    **No server-side state**: There is no "begin" endpoint. The client manages
    its own read set and submits the full transaction in one request.

        Attributes:
            read_set (list['TransactionReadItem']): Set of keys that were read during the transaction, with their observed
                versions.
                The server verifies these versions still match before committing writes.
            tables (TransactionCommitRequestTables): Write set: map of table names to batch operations, same format as
                MultiBatchRequest.tables.
            sync_level (Union[Unset, SyncLevel]): Synchronization level for batch operations:
                - "propose": Wait for Raft proposal acceptance (fastest, default)
                - "write": Wait for Pebble KV write
                - "full_text": Wait for full-text index WAL write
                - "enrichments": Pre-compute enrichments before Raft proposal (synchronous enrichment generation)
                - "aknn": Wait for vector index write with best-effort synchronous embedding (falls back to async on timeout,
                slowest, most durable)
    """

    read_set: list["TransactionReadItem"]
    tables: "TransactionCommitRequestTables"
    sync_level: Union[Unset, SyncLevel] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        read_set = []
        for read_set_item_data in self.read_set:
            read_set_item = read_set_item_data.to_dict()
            read_set.append(read_set_item)

        tables = self.tables.to_dict()

        sync_level: Union[Unset, str] = UNSET
        if not isinstance(self.sync_level, Unset):
            sync_level = self.sync_level.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "read_set": read_set,
                "tables": tables,
            }
        )
        if sync_level is not UNSET:
            field_dict["sync_level"] = sync_level

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.transaction_commit_request_tables import TransactionCommitRequestTables
        from ..models.transaction_read_item import TransactionReadItem

        d = dict(src_dict)
        read_set = []
        _read_set = d.pop("read_set")
        for read_set_item_data in _read_set:
            read_set_item = TransactionReadItem.from_dict(read_set_item_data)

            read_set.append(read_set_item)

        tables = TransactionCommitRequestTables.from_dict(d.pop("tables"))

        _sync_level = d.pop("sync_level", UNSET)
        sync_level: Union[Unset, SyncLevel]
        if isinstance(_sync_level, Unset):
            sync_level = UNSET
        else:
            sync_level = SyncLevel(_sync_level)

        transaction_commit_request = cls(
            read_set=read_set,
            tables=tables,
            sync_level=sync_level,
        )

        transaction_commit_request.additional_properties = d
        return transaction_commit_request

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
