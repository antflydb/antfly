from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sync_level import SyncLevel
from ..types import UNSET, Unset

T = TypeVar("T", bound="TransactionBeginRequest")


@_attrs_define
class TransactionBeginRequest:
    """
    Attributes:
        sync_level (SyncLevel | Unset): Synchronization level for batch operations:
            - "propose": Wait for Raft proposal acceptance (fastest, default)
            - "write": Wait for Pebble KV write
            - "full_text": Wait for full-text index WAL write
            - "enrichments": Pre-compute enrichments before Raft proposal (synchronous enrichment generation)
            - "aknn": Wait for vector index write with best-effort synchronous embedding (falls back to async on timeout,
            slowest, most durable)
            - "full_index": Wait for all index writes to complete (full-text + enrichments + aknn)
    """

    sync_level: SyncLevel | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        sync_level: str | Unset = UNSET
        if not isinstance(self.sync_level, Unset):
            sync_level = self.sync_level.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if sync_level is not UNSET:
            field_dict["sync_level"] = sync_level

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _sync_level = d.pop("sync_level", UNSET)
        sync_level: SyncLevel | Unset
        if isinstance(_sync_level, Unset):
            sync_level = UNSET
        else:
            sync_level = SyncLevel(_sync_level)

        transaction_begin_request = cls(
            sync_level=sync_level,
        )

        transaction_begin_request.additional_properties = d
        return transaction_begin_request

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
