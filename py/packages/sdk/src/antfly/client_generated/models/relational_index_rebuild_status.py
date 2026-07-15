from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalIndexRebuildStatus")


@_attrs_define
class RelationalIndexRebuildStatus:
    """Aggregate secondary-index rebuild progress projected from authoritative metadata ranges.

    Attributes:
        range_count (int | Unset):
        matching_generation_range_count (int | Unset):
        stale_generation_range_count (int | Unset):
        declared_range_count (int | Unset):
        building_range_count (int | Unset):
        ready_range_count (int | Unset):
        invalid_range_count (int | Unset):
        completed_row_count (int | Unset):
        progress_row_key (str | Unset):
        last_error (str | Unset):
    """

    range_count: int | Unset = UNSET
    matching_generation_range_count: int | Unset = UNSET
    stale_generation_range_count: int | Unset = UNSET
    declared_range_count: int | Unset = UNSET
    building_range_count: int | Unset = UNSET
    ready_range_count: int | Unset = UNSET
    invalid_range_count: int | Unset = UNSET
    completed_row_count: int | Unset = UNSET
    progress_row_key: str | Unset = UNSET
    last_error: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        range_count = self.range_count

        matching_generation_range_count = self.matching_generation_range_count

        stale_generation_range_count = self.stale_generation_range_count

        declared_range_count = self.declared_range_count

        building_range_count = self.building_range_count

        ready_range_count = self.ready_range_count

        invalid_range_count = self.invalid_range_count

        completed_row_count = self.completed_row_count

        progress_row_key = self.progress_row_key

        last_error = self.last_error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if range_count is not UNSET:
            field_dict["range_count"] = range_count
        if matching_generation_range_count is not UNSET:
            field_dict["matching_generation_range_count"] = matching_generation_range_count
        if stale_generation_range_count is not UNSET:
            field_dict["stale_generation_range_count"] = stale_generation_range_count
        if declared_range_count is not UNSET:
            field_dict["declared_range_count"] = declared_range_count
        if building_range_count is not UNSET:
            field_dict["building_range_count"] = building_range_count
        if ready_range_count is not UNSET:
            field_dict["ready_range_count"] = ready_range_count
        if invalid_range_count is not UNSET:
            field_dict["invalid_range_count"] = invalid_range_count
        if completed_row_count is not UNSET:
            field_dict["completed_row_count"] = completed_row_count
        if progress_row_key is not UNSET:
            field_dict["progress_row_key"] = progress_row_key
        if last_error is not UNSET:
            field_dict["last_error"] = last_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        range_count = d.pop("range_count", UNSET)

        matching_generation_range_count = d.pop("matching_generation_range_count", UNSET)

        stale_generation_range_count = d.pop("stale_generation_range_count", UNSET)

        declared_range_count = d.pop("declared_range_count", UNSET)

        building_range_count = d.pop("building_range_count", UNSET)

        ready_range_count = d.pop("ready_range_count", UNSET)

        invalid_range_count = d.pop("invalid_range_count", UNSET)

        completed_row_count = d.pop("completed_row_count", UNSET)

        progress_row_key = d.pop("progress_row_key", UNSET)

        last_error = d.pop("last_error", UNSET)

        relational_index_rebuild_status = cls(
            range_count=range_count,
            matching_generation_range_count=matching_generation_range_count,
            stale_generation_range_count=stale_generation_range_count,
            declared_range_count=declared_range_count,
            building_range_count=building_range_count,
            ready_range_count=ready_range_count,
            invalid_range_count=invalid_range_count,
            completed_row_count=completed_row_count,
            progress_row_key=progress_row_key,
            last_error=last_error,
        )

        relational_index_rebuild_status.additional_properties = d
        return relational_index_rebuild_status

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
