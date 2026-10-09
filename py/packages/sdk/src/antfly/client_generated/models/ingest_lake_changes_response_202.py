from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="IngestLakeChangesResponse202")


@_attrs_define
class IngestLakeChangesResponse202:
    """
    Attributes:
        state (str | Unset):
        wal_lsn (int | Unset):
        table_id (int | Unset):
        object_generation (int | Unset):
        searchable (bool | Unset):
    """

    state: str | Unset = UNSET
    wal_lsn: int | Unset = UNSET
    table_id: int | Unset = UNSET
    object_generation: int | Unset = UNSET
    searchable: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        state = self.state

        wal_lsn = self.wal_lsn

        table_id = self.table_id

        object_generation = self.object_generation

        searchable = self.searchable

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if state is not UNSET:
            field_dict["state"] = state
        if wal_lsn is not UNSET:
            field_dict["wal_lsn"] = wal_lsn
        if table_id is not UNSET:
            field_dict["table_id"] = table_id
        if object_generation is not UNSET:
            field_dict["object_generation"] = object_generation
        if searchable is not UNSET:
            field_dict["searchable"] = searchable

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        state = d.pop("state", UNSET)

        wal_lsn = d.pop("wal_lsn", UNSET)

        table_id = d.pop("table_id", UNSET)

        object_generation = d.pop("object_generation", UNSET)

        searchable = d.pop("searchable", UNSET)

        ingest_lake_changes_response_202 = cls(
            state=state,
            wal_lsn=wal_lsn,
            table_id=table_id,
            object_generation=object_generation,
            searchable=searchable,
        )

        ingest_lake_changes_response_202.additional_properties = d
        return ingest_lake_changes_response_202

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
