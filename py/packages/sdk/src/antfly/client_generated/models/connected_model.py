from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ConnectedModel")


@_attrs_define
class ConnectedModel:
    """
    Attributes:
        name (str): Model identifier as reported by the provider.
        display_name (str | Unset): Human-readable model name when the provider reports one.
        dimensions (int | Unset): Embedding output dimension when known.
        configured (bool | Unset): True when this model is referenced by a configured embedder, generator, reranker, or
            chunker.
    """

    name: str
    display_name: str | Unset = UNSET
    dimensions: int | Unset = UNSET
    configured: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        display_name = self.display_name

        dimensions = self.dimensions

        configured = self.configured

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if display_name is not UNSET:
            field_dict["display_name"] = display_name
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if configured is not UNSET:
            field_dict["configured"] = configured

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        display_name = d.pop("display_name", UNSET)

        dimensions = d.pop("dimensions", UNSET)

        configured = d.pop("configured", UNSET)

        connected_model = cls(
            name=name,
            display_name=display_name,
            dimensions=dimensions,
            configured=configured,
        )

        connected_model.additional_properties = d
        return connected_model

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
