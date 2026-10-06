from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.connector_capabilities_chatgpt_reason import ConnectorCapabilitiesChatgptReason
from ..types import UNSET, Unset

T = TypeVar("T", bound="ConnectorCapabilitiesChatgpt")


@_attrs_define
class ConnectorCapabilitiesChatgpt:
    """
    Attributes:
        enabled (bool):
        reason (ConnectorCapabilitiesChatgptReason | Unset):
    """

    enabled: bool
    reason: ConnectorCapabilitiesChatgptReason | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        reason: str | Unset = UNSET
        if not isinstance(self.reason, Unset):
            reason = self.reason.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "enabled": enabled,
            }
        )
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enabled = d.pop("enabled")

        _reason = d.pop("reason", UNSET)
        reason: ConnectorCapabilitiesChatgptReason | Unset
        if isinstance(_reason, Unset):
            reason = UNSET
        else:
            reason = ConnectorCapabilitiesChatgptReason(_reason)

        connector_capabilities_chatgpt = cls(
            enabled=enabled,
            reason=reason,
        )

        return connector_capabilities_chatgpt
