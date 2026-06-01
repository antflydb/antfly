from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ReplicationSourceStatus")


@_attrs_define
class ReplicationSourceStatus:
    """Runtime status of this replication source. Present only in GET table
    detail responses, not in create/update requests.

        Attributes:
            source_kind (str | Unset):
            external_table (str | Unset):
            cutover_mode (str | Unset):
            slot_name (str | Unset):
            publication_name (str | Unset):
            phase (str | Unset):
            checkpoint (str | Unset):
            snapshot_offset (int | Unset):
            prepared_checkpoint (str | Unset):
            stream_checkpoint (str | Unset):
            last_error (str | Unset):
            failure_class (str | Unset):
            lag_records (int | Unset):
            lag_millis (int | Unset):
            consecutive_failures (int | Unset):
            last_source_commit_at_ms (int | Unset):
            last_success_at_ms (int | Unset):
            last_change_applied_at_ms (int | Unset):
            updated_at_ms (int | Unset):
    """

    source_kind: str | Unset = UNSET
    external_table: str | Unset = UNSET
    cutover_mode: str | Unset = UNSET
    slot_name: str | Unset = UNSET
    publication_name: str | Unset = UNSET
    phase: str | Unset = UNSET
    checkpoint: str | Unset = UNSET
    snapshot_offset: int | Unset = UNSET
    prepared_checkpoint: str | Unset = UNSET
    stream_checkpoint: str | Unset = UNSET
    last_error: str | Unset = UNSET
    failure_class: str | Unset = UNSET
    lag_records: int | Unset = UNSET
    lag_millis: int | Unset = UNSET
    consecutive_failures: int | Unset = UNSET
    last_source_commit_at_ms: int | Unset = UNSET
    last_success_at_ms: int | Unset = UNSET
    last_change_applied_at_ms: int | Unset = UNSET
    updated_at_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source_kind = self.source_kind

        external_table = self.external_table

        cutover_mode = self.cutover_mode

        slot_name = self.slot_name

        publication_name = self.publication_name

        phase = self.phase

        checkpoint = self.checkpoint

        snapshot_offset = self.snapshot_offset

        prepared_checkpoint = self.prepared_checkpoint

        stream_checkpoint = self.stream_checkpoint

        last_error = self.last_error

        failure_class = self.failure_class

        lag_records = self.lag_records

        lag_millis = self.lag_millis

        consecutive_failures = self.consecutive_failures

        last_source_commit_at_ms = self.last_source_commit_at_ms

        last_success_at_ms = self.last_success_at_ms

        last_change_applied_at_ms = self.last_change_applied_at_ms

        updated_at_ms = self.updated_at_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if source_kind is not UNSET:
            field_dict["source_kind"] = source_kind
        if external_table is not UNSET:
            field_dict["external_table"] = external_table
        if cutover_mode is not UNSET:
            field_dict["cutover_mode"] = cutover_mode
        if slot_name is not UNSET:
            field_dict["slot_name"] = slot_name
        if publication_name is not UNSET:
            field_dict["publication_name"] = publication_name
        if phase is not UNSET:
            field_dict["phase"] = phase
        if checkpoint is not UNSET:
            field_dict["checkpoint"] = checkpoint
        if snapshot_offset is not UNSET:
            field_dict["snapshot_offset"] = snapshot_offset
        if prepared_checkpoint is not UNSET:
            field_dict["prepared_checkpoint"] = prepared_checkpoint
        if stream_checkpoint is not UNSET:
            field_dict["stream_checkpoint"] = stream_checkpoint
        if last_error is not UNSET:
            field_dict["last_error"] = last_error
        if failure_class is not UNSET:
            field_dict["failure_class"] = failure_class
        if lag_records is not UNSET:
            field_dict["lag_records"] = lag_records
        if lag_millis is not UNSET:
            field_dict["lag_millis"] = lag_millis
        if consecutive_failures is not UNSET:
            field_dict["consecutive_failures"] = consecutive_failures
        if last_source_commit_at_ms is not UNSET:
            field_dict["last_source_commit_at_ms"] = last_source_commit_at_ms
        if last_success_at_ms is not UNSET:
            field_dict["last_success_at_ms"] = last_success_at_ms
        if last_change_applied_at_ms is not UNSET:
            field_dict["last_change_applied_at_ms"] = last_change_applied_at_ms
        if updated_at_ms is not UNSET:
            field_dict["updated_at_ms"] = updated_at_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        source_kind = d.pop("source_kind", UNSET)

        external_table = d.pop("external_table", UNSET)

        cutover_mode = d.pop("cutover_mode", UNSET)

        slot_name = d.pop("slot_name", UNSET)

        publication_name = d.pop("publication_name", UNSET)

        phase = d.pop("phase", UNSET)

        checkpoint = d.pop("checkpoint", UNSET)

        snapshot_offset = d.pop("snapshot_offset", UNSET)

        prepared_checkpoint = d.pop("prepared_checkpoint", UNSET)

        stream_checkpoint = d.pop("stream_checkpoint", UNSET)

        last_error = d.pop("last_error", UNSET)

        failure_class = d.pop("failure_class", UNSET)

        lag_records = d.pop("lag_records", UNSET)

        lag_millis = d.pop("lag_millis", UNSET)

        consecutive_failures = d.pop("consecutive_failures", UNSET)

        last_source_commit_at_ms = d.pop("last_source_commit_at_ms", UNSET)

        last_success_at_ms = d.pop("last_success_at_ms", UNSET)

        last_change_applied_at_ms = d.pop("last_change_applied_at_ms", UNSET)

        updated_at_ms = d.pop("updated_at_ms", UNSET)

        replication_source_status = cls(
            source_kind=source_kind,
            external_table=external_table,
            cutover_mode=cutover_mode,
            slot_name=slot_name,
            publication_name=publication_name,
            phase=phase,
            checkpoint=checkpoint,
            snapshot_offset=snapshot_offset,
            prepared_checkpoint=prepared_checkpoint,
            stream_checkpoint=stream_checkpoint,
            last_error=last_error,
            failure_class=failure_class,
            lag_records=lag_records,
            lag_millis=lag_millis,
            consecutive_failures=consecutive_failures,
            last_source_commit_at_ms=last_source_commit_at_ms,
            last_success_at_ms=last_success_at_ms,
            last_change_applied_at_ms=last_change_applied_at_ms,
            updated_at_ms=updated_at_ms,
        )

        replication_source_status.additional_properties = d
        return replication_source_status

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
