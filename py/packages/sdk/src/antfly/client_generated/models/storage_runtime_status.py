from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.storage_runtime_status_engine import StorageRuntimeStatusEngine
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.storage_maintenance_capabilities import StorageMaintenanceCapabilities


T = TypeVar("T", bound="StorageRuntimeStatus")


@_attrs_define
class StorageRuntimeStatus:
    """
    Attributes:
        engine (StorageRuntimeStatusEngine):
        maintenance (StorageMaintenanceCapabilities):
        format_ (str | Unset):
        fsync (bool | Unset):
    """

    engine: StorageRuntimeStatusEngine
    maintenance: StorageMaintenanceCapabilities
    format_: str | Unset = UNSET
    fsync: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        engine = self.engine.value

        maintenance = self.maintenance.to_dict()

        format_ = self.format_

        fsync = self.fsync

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "engine": engine,
                "maintenance": maintenance,
            }
        )
        if format_ is not UNSET:
            field_dict["format"] = format_
        if fsync is not UNSET:
            field_dict["fsync"] = fsync

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.storage_maintenance_capabilities import StorageMaintenanceCapabilities

        d = dict(src_dict)
        engine = StorageRuntimeStatusEngine(d.pop("engine"))

        maintenance = StorageMaintenanceCapabilities.from_dict(d.pop("maintenance"))

        format_ = d.pop("format", UNSET)

        fsync = d.pop("fsync", UNSET)

        storage_runtime_status = cls(
            engine=engine,
            maintenance=maintenance,
            format_=format_,
            fsync=fsync,
        )

        return storage_runtime_status
