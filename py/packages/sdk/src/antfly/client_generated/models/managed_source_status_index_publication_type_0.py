from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ManagedSourceStatusIndexPublicationType0")


@_attrs_define
class ManagedSourceStatusIndexPublicationType0:
    """
    Attributes:
        published_generation (int | None | Unset):
        published_snapshot (None | str | Unset):
        building_generation (int | None | Unset):
        failed_generation (int | None | Unset):
    """

    published_generation: int | None | Unset = UNSET
    published_snapshot: None | str | Unset = UNSET
    building_generation: int | None | Unset = UNSET
    failed_generation: int | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        published_generation: int | None | Unset
        if isinstance(self.published_generation, Unset):
            published_generation = UNSET
        else:
            published_generation = self.published_generation

        published_snapshot: None | str | Unset
        if isinstance(self.published_snapshot, Unset):
            published_snapshot = UNSET
        else:
            published_snapshot = self.published_snapshot

        building_generation: int | None | Unset
        if isinstance(self.building_generation, Unset):
            building_generation = UNSET
        else:
            building_generation = self.building_generation

        failed_generation: int | None | Unset
        if isinstance(self.failed_generation, Unset):
            failed_generation = UNSET
        else:
            failed_generation = self.failed_generation

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if published_generation is not UNSET:
            field_dict["published_generation"] = published_generation
        if published_snapshot is not UNSET:
            field_dict["published_snapshot"] = published_snapshot
        if building_generation is not UNSET:
            field_dict["building_generation"] = building_generation
        if failed_generation is not UNSET:
            field_dict["failed_generation"] = failed_generation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_published_generation(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        published_generation = _parse_published_generation(d.pop("published_generation", UNSET))

        def _parse_published_snapshot(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        published_snapshot = _parse_published_snapshot(d.pop("published_snapshot", UNSET))

        def _parse_building_generation(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        building_generation = _parse_building_generation(d.pop("building_generation", UNSET))

        def _parse_failed_generation(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        failed_generation = _parse_failed_generation(d.pop("failed_generation", UNSET))

        managed_source_status_index_publication_type_0 = cls(
            published_generation=published_generation,
            published_snapshot=published_snapshot,
            building_generation=building_generation,
            failed_generation=failed_generation,
        )

        managed_source_status_index_publication_type_0.additional_properties = d
        return managed_source_status_index_publication_type_0

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
