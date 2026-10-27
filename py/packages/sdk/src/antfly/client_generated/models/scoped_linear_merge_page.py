from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sync_level import SyncLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.scoped_linear_merge_page_key_range import ScopedLinearMergePageKeyRange
    from ..models.scoped_linear_merge_page_records import ScopedLinearMergePageRecords


T = TypeVar("T", bound="ScopedLinearMergePage")


@_attrs_define
class ScopedLinearMergePage:
    """Merge within explicit exclusive key bounds. Records and retained IDs
    must be inside the range and after last_merged_id. Overlapping writers
    still require external serialization. Upsert pages normally set
    delete_missing=false. After a complete authoritative inventory has been
    written and verified, an empty-record page with retain_ids reconciles
    the entire bounded range without retransmitting document content.

        Attributes:
            key_range (ScopedLinearMergePageKeyRange):
            records (ScopedLinearMergePageRecords):
            retain_ids (list[str] | Unset): Valid only with empty records. Keep these already-written IDs while
                reconciling the bounded range. Missing retained records abort the
                request before any mutation. Use an empty list for an empty corpus.
            last_merged_id (str | Unset): Exclusive page cursor within key_range; defaults to key_range.from.
            delete_missing (bool | Unset): Explicitly authorize deletion of records absent from this page or retain_ids.
                Default: False.
            dry_run (bool | Unset):  Default: False.
            sync_level (SyncLevel | Unset): Synchronization level for batch operations:
                - "propose": Wait for Raft proposal acceptance (fastest, default)
                - "write": Wait for the write to be durably applied to the local key-value store
                - "full_text": Wait for full-text index WAL write
                - "enrichments": Precompute enrichments before committing the document. A synchronous
                  producer failure rejects the write; post-commit worker failures retain the document
                  and may return `committed_repair_required`.
                - "full_index": Wait for all index writes to complete (full-text + enrichments + vector indexes)
    """

    key_range: ScopedLinearMergePageKeyRange
    records: ScopedLinearMergePageRecords
    retain_ids: list[str] | Unset = UNSET
    last_merged_id: str | Unset = UNSET
    delete_missing: bool | Unset = False
    dry_run: bool | Unset = False
    sync_level: SyncLevel | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key_range = self.key_range.to_dict()

        records = self.records.to_dict()

        retain_ids: list[str] | Unset = UNSET
        if not isinstance(self.retain_ids, Unset):
            retain_ids = self.retain_ids

        last_merged_id = self.last_merged_id

        delete_missing = self.delete_missing

        dry_run = self.dry_run

        sync_level: str | Unset = UNSET
        if not isinstance(self.sync_level, Unset):
            sync_level = self.sync_level.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key_range": key_range,
                "records": records,
            }
        )
        if retain_ids is not UNSET:
            field_dict["retain_ids"] = retain_ids
        if last_merged_id is not UNSET:
            field_dict["last_merged_id"] = last_merged_id
        if delete_missing is not UNSET:
            field_dict["delete_missing"] = delete_missing
        if dry_run is not UNSET:
            field_dict["dry_run"] = dry_run
        if sync_level is not UNSET:
            field_dict["sync_level"] = sync_level

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scoped_linear_merge_page_key_range import ScopedLinearMergePageKeyRange
        from ..models.scoped_linear_merge_page_records import ScopedLinearMergePageRecords

        d = dict(src_dict)
        key_range = ScopedLinearMergePageKeyRange.from_dict(d.pop("key_range"))

        records = ScopedLinearMergePageRecords.from_dict(d.pop("records"))

        retain_ids = cast(list[str], d.pop("retain_ids", UNSET))

        last_merged_id = d.pop("last_merged_id", UNSET)

        delete_missing = d.pop("delete_missing", UNSET)

        dry_run = d.pop("dry_run", UNSET)

        _sync_level = d.pop("sync_level", UNSET)
        sync_level: SyncLevel | Unset
        if isinstance(_sync_level, Unset):
            sync_level = UNSET
        else:
            sync_level = SyncLevel(_sync_level)

        scoped_linear_merge_page = cls(
            key_range=key_range,
            records=records,
            retain_ids=retain_ids,
            last_merged_id=last_merged_id,
            delete_missing=delete_missing,
            dry_run=dry_run,
            sync_level=sync_level,
        )

        scoped_linear_merge_page.additional_properties = d
        return scoped_linear_merge_page

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
