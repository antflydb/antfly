from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.connector_capabilities_chatgpt import ConnectorCapabilitiesChatgpt


T = TypeVar("T", bound="ConnectorCapabilities")


@_attrs_define
class ConnectorCapabilities:
    """Effective integration availability for this deployment; independent of login providers and individual grants.

    Attributes:
        chatgpt (ConnectorCapabilitiesChatgpt):
    """

    chatgpt: ConnectorCapabilitiesChatgpt

    def to_dict(self) -> dict[str, Any]:
        chatgpt = self.chatgpt.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "chatgpt": chatgpt,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.connector_capabilities_chatgpt import ConnectorCapabilitiesChatgpt

        d = dict(src_dict)
        chatgpt = ConnectorCapabilitiesChatgpt.from_dict(d.pop("chatgpt"))

        connector_capabilities = cls(
            chatgpt=chatgpt,
        )

        return connector_capabilities
