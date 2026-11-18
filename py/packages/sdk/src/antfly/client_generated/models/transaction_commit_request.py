from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.commit_requires import CommitRequires
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
            read_set (list[TransactionReadItem]): Set of keys that were read during the transaction, with their observed
                versions.
                The server verifies these versions still match before committing writes.
            tables (TransactionCommitRequestTables): Write set: map of table names to batch operations, same format as
                MultiBatchRequest.tables.
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

    read_set: list[TransactionReadItem]
    tables: TransactionCommitRequestTables
    commit_requires: CommitRequires | Unset = UNSET
    sync_level: SyncLevel | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        read_set = []
        for read_set_item_data in self.read_set:
            read_set_item = read_set_item_data.to_dict()
            read_set.append(read_set_item)

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
                "read_set": read_set,
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
        from ..models.transaction_commit_request_tables import TransactionCommitRequestTables
        from ..models.transaction_read_item import TransactionReadItem

        d = dict(src_dict)
        read_set = []
        _read_set = d.pop("read_set")
        for read_set_item_data in _read_set:
            read_set_item = TransactionReadItem.from_dict(read_set_item_data)

            read_set.append(read_set_item)

        tables = TransactionCommitRequestTables.from_dict(d.pop("tables"))

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

        transaction_commit_request = cls(
            read_set=read_set,
            tables=tables,
            commit_requires=commit_requires,
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
