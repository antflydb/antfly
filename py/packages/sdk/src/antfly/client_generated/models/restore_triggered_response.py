from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.restore_triggered_response_restore import RestoreTriggeredResponseRestore

T = TypeVar("T", bound="RestoreTriggeredResponse")


@_attrs_define
class RestoreTriggeredResponse:
    """
    Attributes:
        restore (RestoreTriggeredResponseRestore):
    """

    restore: RestoreTriggeredResponseRestore

    def to_dict(self) -> dict[str, Any]:
        restore = self.restore.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "restore": restore,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        restore = RestoreTriggeredResponseRestore(d.pop("restore"))

        restore_triggered_response = cls(
            restore=restore,
        )

        return restore_triggered_response
