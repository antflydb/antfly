from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sync_level import SyncLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.batch_request_inserts import BatchRequestInserts
    from ..models.transform import Transform


T = TypeVar("T", bound="BatchRequest")


@_attrs_define
class BatchRequest:
    """Batch insert, delete, and transform operations in a single request.

    **Atomicity**:
    - **Single shard**: Operations are atomic within shard boundaries
    - **Multiple shards**: Uses distributed 2-phase commit (2PC) for atomic cross-shard writes

    **How distributed transactions work**:
    1. Metadata server allocates HLC timestamp and selects coordinator shard
    2. Coordinator writes transaction record, participants write intents
    3. After all intents succeed, coordinator commits transaction
    4. Participants are notified asynchronously to resolve intents
    5. Recovery loop ensures notifications complete even after coordinator failure

    **Performance**:
    - Single-shard batches: < 5ms latency
    - Cross-shard transactions: ~20ms latency
    - Intent resolution: < 30 seconds worst-case (via recovery loop)

    **Guarantees**:
    - All writes succeed or all fail (atomicity across all shards)
    - Coordinator failure is recoverable (new leader resumes notifications)
    - Idempotent resolution (duplicate notifications are safe)

    **Benefits**:
    - Reduces network overhead compared to individual requests
    - More efficient indexing (updates are batched)
    - Automatic distributed transactions when operations span shards

    The inserts are upserts - existing keys are overwritten, new keys are created.

        Example:
            {'inserts': {'user:123': {'name': 'John Doe', 'email': 'john@example.com', 'age': 30, 'tags': ['customer',
                'premium']}, 'user:456': {'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25, 'tags': ['customer']}},
                'deletes': ['user:789', 'user:old_account']}

        Attributes:
            inserts (Union[Unset, BatchRequestInserts]): Map of document IDs to document objects. Each key is the unique
                identifier for the document.

                Best practices:
                - Use consistent key naming schemes (e.g., "user:123", "article:456")
                - Key length affects storage and performance - keep them reasonably short
                - Keys are sorted lexicographically, so choose prefixes that support range scans
                 Example: {'user:123': {'name': 'John Doe', 'email': 'john@example.com', 'age': 30, 'tags': ['customer',
                'premium']}, 'user:456': {'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25, 'tags': ['customer']}}.
            deletes (Union[Unset, list[str]]): Array of document IDs to delete. Documents are removed from all indexes.

                Notes:
                - Non-existent keys are silently ignored
                - Deletions are processed before inserts in the same batch
                - Keys are permanently removed from storage and indexes
                 Example: ['user:789', 'user:old_account'].
            transforms (Union[Unset, list['Transform']]): Array of transform operations for in-place document updates using
                MongoDB-style operators.

                Transform operations allow you to modify documents without read-modify-write races:
                - Operations are applied atomically on the server
                - Multiple operations per document are applied in sequence
                - Supports numeric operations ($inc, $mul), array operations ($push, $pull), and more

                Common use cases:
                - Increment counters (views, likes, votes)
                - Update timestamps ($currentDate)
                - Manage arrays (add/remove tags, items)
                - Update nested fields without overwriting the entire document
                 Example: [{'key': 'article:123', 'operations': [{'op': '$inc', 'path': '$.views', 'value': 1}, {'op':
                '$currentDate', 'path': '$.lastViewed'}]}, {'key': 'user:456', 'operations': [{'op': '$push', 'path': '$.tags',
                'value': 'vip'}]}].
            sync_level (Union[Unset, SyncLevel]): Synchronization level for batch operations:
                - "propose": Wait for Raft proposal acceptance (fastest, default)
                - "write": Wait for Pebble KV write
                - "full_text": Wait for full-text index WAL write
                - "enrichments": Pre-compute enrichments before Raft proposal (synchronous enrichment generation)
                - "aknn": Wait for vector index write with best-effort synchronous embedding (falls back to async on timeout,
                slowest, most durable)
    """

    inserts: Union[Unset, "BatchRequestInserts"] = UNSET
    deletes: Union[Unset, list[str]] = UNSET
    transforms: Union[Unset, list["Transform"]] = UNSET
    sync_level: Union[Unset, SyncLevel] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        inserts: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.inserts, Unset):
            inserts = self.inserts.to_dict()

        deletes: Union[Unset, list[str]] = UNSET
        if not isinstance(self.deletes, Unset):
            deletes = self.deletes

        transforms: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.transforms, Unset):
            transforms = []
            for transforms_item_data in self.transforms:
                transforms_item = transforms_item_data.to_dict()
                transforms.append(transforms_item)

        sync_level: Union[Unset, str] = UNSET
        if not isinstance(self.sync_level, Unset):
            sync_level = self.sync_level.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if inserts is not UNSET:
            field_dict["inserts"] = inserts
        if deletes is not UNSET:
            field_dict["deletes"] = deletes
        if transforms is not UNSET:
            field_dict["transforms"] = transforms
        if sync_level is not UNSET:
            field_dict["sync_level"] = sync_level

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.batch_request_inserts import BatchRequestInserts
        from ..models.transform import Transform

        d = dict(src_dict)
        _inserts = d.pop("inserts", UNSET)
        inserts: Union[Unset, BatchRequestInserts]
        if isinstance(_inserts, Unset):
            inserts = UNSET
        else:
            inserts = BatchRequestInserts.from_dict(_inserts)

        deletes = cast(list[str], d.pop("deletes", UNSET))

        transforms = []
        _transforms = d.pop("transforms", UNSET)
        for transforms_item_data in _transforms or []:
            transforms_item = Transform.from_dict(transforms_item_data)

            transforms.append(transforms_item)

        _sync_level = d.pop("sync_level", UNSET)
        sync_level: Union[Unset, SyncLevel]
        if isinstance(_sync_level, Unset):
            sync_level = UNSET
        else:
            sync_level = SyncLevel(_sync_level)

        batch_request = cls(
            inserts=inserts,
            deletes=deletes,
            transforms=transforms,
            sync_level=sync_level,
        )

        batch_request.additional_properties = d
        return batch_request

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
