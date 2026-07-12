from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.restore_accepted_response_type_0_restore import RestoreAcceptedResponseType0Restore

T = TypeVar("T", bound="RestoreAcceptedResponseType0")


@_attrs_define
class RestoreAcceptedResponseType0:
    """
    Attributes:
        restore (RestoreAcceptedResponseType0Restore):
    """

    restore: RestoreAcceptedResponseType0Restore

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
        restore = RestoreAcceptedResponseType0Restore(d.pop("restore"))

        restore_accepted_response_type_0 = cls(
            restore=restore,
        )

        return restore_accepted_response_type_0
