from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DependencyImpactRow")


@_attrs_define
class DependencyImpactRow:
    """
    Attributes:
        table_id (str):
        key (str):
        artifact (str):
        freshness (str):
        path (list[str]):
        table (str | Unset): Public name of the dependent table, when visible to the caller.
        version (int | Unset):
        historical (bool | Unset):
        live_candidate (bool | Unset):
    """

    table_id: str
    key: str
    artifact: str
    freshness: str
    path: list[str]
    table: str | Unset = UNSET
    version: int | Unset = UNSET
    historical: bool | Unset = UNSET
    live_candidate: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table_id = self.table_id

        key = self.key

        artifact = self.artifact

        freshness = self.freshness

        path = self.path

        table = self.table

        version = self.version

        historical = self.historical

        live_candidate = self.live_candidate

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table_id": table_id,
                "key": key,
                "artifact": artifact,
                "freshness": freshness,
                "path": path,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table
        if version is not UNSET:
            field_dict["version"] = version
        if historical is not UNSET:
            field_dict["historical"] = historical
        if live_candidate is not UNSET:
            field_dict["live_candidate"] = live_candidate

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table_id = d.pop("table_id")

        key = d.pop("key")

        artifact = d.pop("artifact")

        freshness = d.pop("freshness")

        path = cast(list[str], d.pop("path"))

        table = d.pop("table", UNSET)

        version = d.pop("version", UNSET)

        historical = d.pop("historical", UNSET)

        live_candidate = d.pop("live_candidate", UNSET)

        dependency_impact_row = cls(
            table_id=table_id,
            key=key,
            artifact=artifact,
            freshness=freshness,
            path=path,
            table=table,
            version=version,
            historical=historical,
            live_candidate=live_candidate,
        )

        dependency_impact_row.additional_properties = d
        return dependency_impact_row

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
