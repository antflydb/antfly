from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.source_value_identity_state import SourceValueIdentityState
from ..types import UNSET, Unset

T = TypeVar("T", bound="SourceValueIdentity")


@_attrs_define
class SourceValueIdentity:
    """
    Attributes:
        state (SourceValueIdentityState): Stored null is a value. Missing has no identity; unchanged and reference omit
            the value from _source.
        identity (str | Unset): Opaque versioned value identity, present for value, unchanged and reference states.
    """

    state: SourceValueIdentityState
    identity: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        state = self.state.value

        identity = self.identity

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "state": state,
            }
        )
        if identity is not UNSET:
            field_dict["identity"] = identity

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        state = SourceValueIdentityState(d.pop("state"))

        identity = d.pop("identity", UNSET)

        source_value_identity = cls(
            state=state,
            identity=identity,
        )

        source_value_identity.additional_properties = d
        return source_value_identity

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
