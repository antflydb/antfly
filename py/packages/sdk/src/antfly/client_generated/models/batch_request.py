from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.commit_requires import CommitRequires
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
            inserts (BatchRequestInserts | Unset): Map of document IDs to document objects. Each key is the unique
                identifier for the document.

                Best practices:
                - Use consistent key naming schemes (e.g., "user:123", "article:456")
                - Key length affects storage and performance - keep them reasonably short
                - Keys are sorted lexicographically, so choose prefixes that support range scans
                 Example: {'user:123': {'name': 'John Doe', 'email': 'john@example.com', 'age': 30, 'tags': ['customer',
                'premium']}, 'user:456': {'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25, 'tags': ['customer']}}.
            deletes (list[str] | Unset): Array of document IDs to delete. Documents are removed from all indexes.

                Notes:
                - Non-existent keys are silently ignored
                - Deletions take precedence over inserts for the same key in one batch
                - Keys are permanently removed from storage and indexes
                 Example: ['user:789', 'user:old_account'].
            transforms (list[Transform] | Unset): Array of transform operations for in-place document updates using MongoDB-
                style operators.

                Transform operations allow you to modify documents without read-modify-write races:
                - Operations are applied atomically on the server
                - Multiple operations per document are applied in sequence
                - Supports numeric and set-like operations ($inc, $min, $max, $addToSet, $pull)

                Common use cases:
                - Increment counters (views, likes, votes)
                - Update timestamps ($set)
                - Add or remove array values ($addToSet, $pull)
                - Update nested fields without overwriting the entire document
                 Example: [{'key': 'article:123', 'operations': [{'op': '$inc', 'path': '$.views', 'value': 1}, {'op': '$set',
                'path': '$.lastViewed', 'value': '2026-07-28T12:00:00Z'}]}, {'key': 'user:456', 'operations': [{'op':
                '$addToSet', 'path': '$.tags', 'value': 'vip'}]}].
            commit_requires (CommitRequires | Unset): Admission prerequisite independent of sync_level. Explicit none
                commits the
                source without precomputing enrichments. direct_enrichments requires eligible
                document-local producers to succeed, committing their outputs and coverage
                atomically with the source; producer failure rejects the whole batch.
                Artifact-source stages, committed graph/neighbor context, grouped producers,
                protected producers and PDF page-image embeddings remain asynchronous.
                Configured producers with no eligible request reject the requirement.
                Prepared outputs are bounded to 8 MiB and 16384 physical rows per batch.
                Currently supported by local embedded batches. Server HTTP, Raft, serverless
                and transaction-resolution surfaces reject explicit requirements before
                admission until their prepared-output protocols are available.
                Omission retains legacy embedded precomputation for enrichments/full_index
                during the compatibility window; other surfaces retain their existing waits.
            sync_level (SyncLevel | Unset): Response acknowledgment/publication barrier, independent of commit_requires:
                - "propose": Wait for Raft proposal acceptance (fastest, default)
                - "write": Wait for durable application to the local key-value store
                - "full_text": Wait for full-text index WAL write
                - "enrichments": Wait for this write's enrichment publication
                - "full_index": Wait for full-text, enrichment and vector-index publication
                A post-commit wait failure never rolls back the source. Terminal background
                failures may return committed_repair_required. During the embedded migration
                window, omitting commit_requires retains legacy precomputation for enrichments
                and full_index; explicit none disables that implicit admission prerequisite.
    """

    inserts: BatchRequestInserts | Unset = UNSET
    deletes: list[str] | Unset = UNSET
    transforms: list[Transform] | Unset = UNSET
    commit_requires: CommitRequires | Unset = UNSET
    sync_level: SyncLevel | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        inserts: dict[str, Any] | Unset = UNSET
        if not isinstance(self.inserts, Unset):
            inserts = self.inserts.to_dict()

        deletes: list[str] | Unset = UNSET
        if not isinstance(self.deletes, Unset):
            deletes = self.deletes

        transforms: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.transforms, Unset):
            transforms = []
            for transforms_item_data in self.transforms:
                transforms_item = transforms_item_data.to_dict()
                transforms.append(transforms_item)

        commit_requires: str | Unset = UNSET
        if not isinstance(self.commit_requires, Unset):
            commit_requires = self.commit_requires.value

        sync_level: str | Unset = UNSET
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
        if commit_requires is not UNSET:
            field_dict["commit_requires"] = commit_requires
        if sync_level is not UNSET:
            field_dict["sync_level"] = sync_level

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.batch_request_inserts import BatchRequestInserts
        from ..models.transform import Transform

        d = dict(src_dict)
        _inserts = d.pop("inserts", UNSET)
        inserts: BatchRequestInserts | Unset
        if isinstance(_inserts, Unset):
            inserts = UNSET
        else:
            inserts = BatchRequestInserts.from_dict(_inserts)

        deletes = cast(list[str], d.pop("deletes", UNSET))

        _transforms = d.pop("transforms", UNSET)
        transforms: list[Transform] | Unset = UNSET
        if _transforms is not UNSET:
            transforms = []
            for transforms_item_data in _transforms:
                transforms_item = Transform.from_dict(transforms_item_data)

                transforms.append(transforms_item)

        _commit_requires = d.pop("commit_requires", UNSET)
        commit_requires: CommitRequires | Unset
        if isinstance(_commit_requires, Unset):
            commit_requires = UNSET
        else:
            commit_requires = CommitRequires(_commit_requires)

        _sync_level = d.pop("sync_level", UNSET)
        sync_level: SyncLevel | Unset
        if isinstance(_sync_level, Unset):
            sync_level = UNSET
        else:
            sync_level = SyncLevel(_sync_level)

        batch_request = cls(
            inserts=inserts,
            deletes=deletes,
            transforms=transforms,
            commit_requires=commit_requires,
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
