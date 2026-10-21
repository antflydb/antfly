from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ResolverRenderResponse")


@_attrs_define
class ResolverRenderResponse:
    """
    Attributes:
        minted_key (str):
        key (str):
        table (str):
        config_generation (int):
        suppressed (bool):
    """

    minted_key: str
    key: str
    table: str
    config_generation: int
    suppressed: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        minted_key = self.minted_key

        key = self.key

        table = self.table

        config_generation = self.config_generation

        suppressed = self.suppressed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "minted_key": minted_key,
                "key": key,
                "table": table,
                "config_generation": config_generation,
                "suppressed": suppressed,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        minted_key = d.pop("minted_key")

        key = d.pop("key")

        table = d.pop("table")

        config_generation = d.pop("config_generation")

        suppressed = d.pop("suppressed")

        resolver_render_response = cls(
            minted_key=minted_key,
            key=key,
            table=table,
            config_generation=config_generation,
            suppressed=suppressed,
        )

        resolver_render_response.additional_properties = d
        return resolver_render_response

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
