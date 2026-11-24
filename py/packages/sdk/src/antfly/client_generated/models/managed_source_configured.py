from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.managed_source_configured_state import ManagedSourceConfiguredState

T = TypeVar("T", bound="ManagedSourceConfigured")


@_attrs_define
class ManagedSourceConfigured:
    """
    Attributes:
        table_id (int):
        sources_hash (str):
        state (ManagedSourceConfiguredState):
    """

    table_id: int
    sources_hash: str
    state: ManagedSourceConfiguredState
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table_id = self.table_id

        sources_hash = self.sources_hash

        state = self.state.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table_id": table_id,
                "sources_hash": sources_hash,
                "state": state,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table_id = d.pop("table_id")

        sources_hash = d.pop("sources_hash")

        state = ManagedSourceConfiguredState(d.pop("state"))

        managed_source_configured = cls(
            table_id=table_id,
            sources_hash=sources_hash,
            state=state,
        )

        managed_source_configured.additional_properties = d
        return managed_source_configured

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
