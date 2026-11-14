from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.commit_requires import CommitRequires
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
            commit_requires (CommitRequires | Unset): Admission prerequisite independent of sync_level. Explicit none
                commits the
                source without precomputing enrichments. direct_enrichments requires eligible
                document-local producers to succeed, committing their outputs and coverage
                atomically with the source; producer failure rejects the whole batch.
                Artifact-source stages, committed graph/neighbor context, grouped producers,
                protected producers and PDF page-image embeddings remain asynchronous.
                Configured producers with no eligible request reject the requirement.
                Direct admission has an 8 MiB and 16384-row budget for the complete mutation,
                including source keys/values, artifact effects, deletions, coverage, revisions,
                counters and replay payload. Both the submitted source envelope and final
                physical effect set must fit; this is not an artifact-only allowance.
                Supported by embedded batches, server/Raft writes and distributed transactions.
                Raft prepares on the leader and replays immutable effects without inference.
                Transactions reserve the complete envelope before voting prepared; their
                conservative reservation can reject a batch below the final physical limit.
                Serverless supports source-local pipeline stages through conditional atomic
                WAL batches (at most 1024 mutations and 8 MiB including WAL framing).
                Backends without that capability reject before source admission.
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

    tables: MultiBatchRequestTables
    commit_requires: CommitRequires | Unset = UNSET
    sync_level: SyncLevel | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        tables = self.tables.to_dict()

        commit_requires: str | Unset = UNSET
        if not isinstance(self.commit_requires, Unset):
            commit_requires = self.commit_requires.value

        sync_level: str | Unset = UNSET
        if not isinstance(self.sync_level, Unset):
            sync_level = self.sync_level.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "tables": tables,
            }
        )
        if commit_requires is not UNSET:
            field_dict["commit_requires"] = commit_requires
        if sync_level is not UNSET:
            field_dict["sync_level"] = sync_level

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.multi_batch_request_tables import MultiBatchRequestTables

        d = dict(src_dict)
        tables = MultiBatchRequestTables.from_dict(d.pop("tables"))

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

        multi_batch_request = cls(
            tables=tables,
            commit_requires=commit_requires,
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
