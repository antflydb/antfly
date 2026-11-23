from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.replication_source import ReplicationSource


T = TypeVar("T", bound="ManagedSourceReplacement")


@_attrs_define
class ManagedSourceReplacement:
    """
    Attributes:
        table_id (int):
        expected_sources_hash (str):
        replication_sources (list[ReplicationSource]):
    """

    table_id: int
    expected_sources_hash: str
    replication_sources: list[ReplicationSource]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table_id = self.table_id

        expected_sources_hash = self.expected_sources_hash

        replication_sources = []
        for replication_sources_item_data in self.replication_sources:
            replication_sources_item = replication_sources_item_data.to_dict()
            replication_sources.append(replication_sources_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table_id": table_id,
                "expected_sources_hash": expected_sources_hash,
                "replication_sources": replication_sources,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.replication_source import ReplicationSource

        d = dict(src_dict)
        table_id = d.pop("table_id")

        expected_sources_hash = d.pop("expected_sources_hash")

        replication_sources = []
        _replication_sources = d.pop("replication_sources")
        for replication_sources_item_data in _replication_sources:
            replication_sources_item = ReplicationSource.from_dict(replication_sources_item_data)

            replication_sources.append(replication_sources_item)

        managed_source_replacement = cls(
            table_id=table_id,
            expected_sources_hash=expected_sources_hash,
            replication_sources=replication_sources,
        )

        managed_source_replacement.additional_properties = d
        return managed_source_replacement

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
