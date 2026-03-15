from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sync_level import SyncLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.linear_merge_request_records import LinearMergeRequestRecords


T = TypeVar("T", bound="LinearMergeRequest")


@_attrs_define
class LinearMergeRequest:
    """Linear merge operation for syncing sorted records from external sources.
    Use this to keep Antfly in sync with an external database or data source.

    **How it works:**
    1. Send sorted records from your external source
    2. Server upserts records that exist in your batch
    3. Server deletes Antfly records in the key range that are absent from your batch
    4. If stopped at shard boundary, use next_cursor for next request

    **WARNING:** Not safe for concurrent operations with overlapping key ranges.

        Attributes:
            records (LinearMergeRequestRecords): Map of resource ID to resource object: {"resource_id_1": {...},
                "resource_id_2": {...}}

                Requirements:
                - Keys must be sorted lexicographically by your client
                - Server will process keys in sorted order
                - Use consistent key naming (e.g., all start with same prefix)

                This format avoids duplicate IDs and matches Antfly's batch write interface.
                 Example: {'product:001': {'name': 'Laptop', 'price': 999.99}, 'product:002': {'name': 'Mouse', 'price': 29.99},
                'product:003': {'name': 'Keyboard', 'price': 79.99}}.
            last_merged_id (Union[Unset, str]): ID of last record from previous merge request.
                - First request: Use empty string ""
                - Subsequent requests: Use next_cursor from previous response
                - Defines lower bound of key range to process

                This enables pagination for large datasets.
                 Example: product:003.
            dry_run (Union[Unset, bool]): If true, returns what would be deleted without making changes.

                Use cases:
                - Validate sync behavior before committing
                - Check which records will be removed
                - Test key range boundaries

                Response includes deleted_ids array when dry_run=true.
                 Default: False.
            sync_level (Union[Unset, SyncLevel]): Synchronization level for batch operations:
                - "propose": Wait for Raft proposal acceptance (fastest, default)
                - "write": Wait for Pebble KV write
                - "full_text": Wait for full-text index WAL write
                - "enrichments": Pre-compute enrichments before Raft proposal (synchronous enrichment generation)
                - "aknn": Wait for vector index write with best-effort synchronous embedding (falls back to async on timeout,
                slowest, most durable)
    """

    records: "LinearMergeRequestRecords"
    last_merged_id: Union[Unset, str] = UNSET
    dry_run: Union[Unset, bool] = False
    sync_level: Union[Unset, SyncLevel] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        records = self.records.to_dict()

        last_merged_id = self.last_merged_id

        dry_run = self.dry_run

        sync_level: Union[Unset, str] = UNSET
        if not isinstance(self.sync_level, Unset):
            sync_level = self.sync_level.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "records": records,
            }
        )
        if last_merged_id is not UNSET:
            field_dict["last_merged_id"] = last_merged_id
        if dry_run is not UNSET:
            field_dict["dry_run"] = dry_run
        if sync_level is not UNSET:
            field_dict["sync_level"] = sync_level

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.linear_merge_request_records import LinearMergeRequestRecords

        d = dict(src_dict)
        records = LinearMergeRequestRecords.from_dict(d.pop("records"))

        last_merged_id = d.pop("last_merged_id", UNSET)

        dry_run = d.pop("dry_run", UNSET)

        _sync_level = d.pop("sync_level", UNSET)
        sync_level: Union[Unset, SyncLevel]
        if isinstance(_sync_level, Unset):
            sync_level = UNSET
        else:
            sync_level = SyncLevel(_sync_level)

        linear_merge_request = cls(
            records=records,
            last_merged_id=last_merged_id,
            dry_run=dry_run,
            sync_level=sync_level,
        )

        linear_merge_request.additional_properties = d
        return linear_merge_request

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
