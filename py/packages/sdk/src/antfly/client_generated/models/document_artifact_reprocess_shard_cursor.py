from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentArtifactReprocessShardCursor")


@_attrs_define
class DocumentArtifactReprocessShardCursor:
    """
    Attributes:
        next_key (str): Source key cursor for resuming this shard-local repair pass.
        scanned (int): Number of source rows scanned by this shard-local pass.
        reprocessed (int): Number of source rows whose artifact was reprocessed by this shard-local pass.
        skipped (int): Number of scanned source rows that no longer had a reprocessable source document in this shard-
            local pass.
        failed (int): Number of scanned source rows that failed in this shard-local pass.
        limit (int): Effective scan limit used by this shard-local pass.
        group_id (int | None | Unset): Physical table group that produced this cursor, when known.
    """

    next_key: str
    scanned: int
    reprocessed: int
    skipped: int
    failed: int
    limit: int
    group_id: int | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        next_key = self.next_key

        scanned = self.scanned

        reprocessed = self.reprocessed

        skipped = self.skipped

        failed = self.failed

        limit = self.limit

        group_id: int | None | Unset
        if isinstance(self.group_id, Unset):
            group_id = UNSET
        else:
            group_id = self.group_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "next_key": next_key,
                "scanned": scanned,
                "reprocessed": reprocessed,
                "skipped": skipped,
                "failed": failed,
                "limit": limit,
            }
        )
        if group_id is not UNSET:
            field_dict["group_id"] = group_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        next_key = d.pop("next_key")

        scanned = d.pop("scanned")

        reprocessed = d.pop("reprocessed")

        skipped = d.pop("skipped")

        failed = d.pop("failed")

        limit = d.pop("limit")

        def _parse_group_id(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        group_id = _parse_group_id(d.pop("group_id", UNSET))

        document_artifact_reprocess_shard_cursor = cls(
            next_key=next_key,
            scanned=scanned,
            reprocessed=reprocessed,
            skipped=skipped,
            failed=failed,
            limit=limit,
            group_id=group_id,
        )

        document_artifact_reprocess_shard_cursor.additional_properties = d
        return document_artifact_reprocess_shard_cursor

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
