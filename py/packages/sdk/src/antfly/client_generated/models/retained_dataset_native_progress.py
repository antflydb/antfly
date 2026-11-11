from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RetainedDatasetNativeProgress")


@_attrs_define
class RetainedDatasetNativeProgress:
    """
    Attributes:
        ready (bool):
        draining (bool):
        closed (bool):
        archive_handoff (bool):
        backfill_cleanup_complete (bool):
        source_sequence (str):
        replay_floor (str):
        replay_latest (str):
        retention_clock_ns (str):
        last_attempt_ns (None | str | Unset):
        last_error (None | str | Unset): Durable error name from the most recent failed maintenance operation; cleared
            by successful maintenance.
    """

    ready: bool
    draining: bool
    closed: bool
    archive_handoff: bool
    backfill_cleanup_complete: bool
    source_sequence: str
    replay_floor: str
    replay_latest: str
    retention_clock_ns: str
    last_attempt_ns: None | str | Unset = UNSET
    last_error: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        ready = self.ready

        draining = self.draining

        closed = self.closed

        archive_handoff = self.archive_handoff

        backfill_cleanup_complete = self.backfill_cleanup_complete

        source_sequence = self.source_sequence

        replay_floor = self.replay_floor

        replay_latest = self.replay_latest

        retention_clock_ns = self.retention_clock_ns

        last_attempt_ns: None | str | Unset
        if isinstance(self.last_attempt_ns, Unset):
            last_attempt_ns = UNSET
        else:
            last_attempt_ns = self.last_attempt_ns

        last_error: None | str | Unset
        if isinstance(self.last_error, Unset):
            last_error = UNSET
        else:
            last_error = self.last_error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "ready": ready,
                "draining": draining,
                "closed": closed,
                "archive_handoff": archive_handoff,
                "backfill_cleanup_complete": backfill_cleanup_complete,
                "source_sequence": source_sequence,
                "replay_floor": replay_floor,
                "replay_latest": replay_latest,
                "retention_clock_ns": retention_clock_ns,
            }
        )
        if last_attempt_ns is not UNSET:
            field_dict["last_attempt_ns"] = last_attempt_ns
        if last_error is not UNSET:
            field_dict["last_error"] = last_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        ready = d.pop("ready")

        draining = d.pop("draining")

        closed = d.pop("closed")

        archive_handoff = d.pop("archive_handoff")

        backfill_cleanup_complete = d.pop("backfill_cleanup_complete")

        source_sequence = d.pop("source_sequence")

        replay_floor = d.pop("replay_floor")

        replay_latest = d.pop("replay_latest")

        retention_clock_ns = d.pop("retention_clock_ns")

        def _parse_last_attempt_ns(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        last_attempt_ns = _parse_last_attempt_ns(d.pop("last_attempt_ns", UNSET))

        def _parse_last_error(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        last_error = _parse_last_error(d.pop("last_error", UNSET))

        retained_dataset_native_progress = cls(
            ready=ready,
            draining=draining,
            closed=closed,
            archive_handoff=archive_handoff,
            backfill_cleanup_complete=backfill_cleanup_complete,
            source_sequence=source_sequence,
            replay_floor=replay_floor,
            replay_latest=replay_latest,
            retention_clock_ns=retention_clock_ns,
            last_attempt_ns=last_attempt_ns,
            last_error=last_error,
        )

        retained_dataset_native_progress.additional_properties = d
        return retained_dataset_native_progress

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
