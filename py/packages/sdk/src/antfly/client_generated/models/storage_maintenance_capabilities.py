from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="StorageMaintenanceCapabilities")


@_attrs_define
class StorageMaintenanceCapabilities:
    """
    Attributes:
        check (bool):
        compact (bool):
        vacuum (bool):
        online (bool):
        asynchronous (bool):
    """

    check: bool
    compact: bool
    vacuum: bool
    online: bool
    asynchronous: bool

    def to_dict(self) -> dict[str, Any]:
        check = self.check

        compact = self.compact

        vacuum = self.vacuum

        online = self.online

        asynchronous = self.asynchronous

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "check": check,
                "compact": compact,
                "vacuum": vacuum,
                "online": online,
                "asynchronous": asynchronous,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        check = d.pop("check")

        compact = d.pop("compact")

        vacuum = d.pop("vacuum")

        online = d.pop("online")

        asynchronous = d.pop("asynchronous")

        storage_maintenance_capabilities = cls(
            check=check,
            compact=compact,
            vacuum=vacuum,
            online=online,
            asynchronous=asynchronous,
        )

        return storage_maintenance_capabilities
