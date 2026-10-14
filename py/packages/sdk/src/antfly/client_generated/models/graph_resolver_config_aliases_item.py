from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphResolverConfigAliasesItem")


@_attrs_define
class GraphResolverConfigAliasesItem:
    """
    Attributes:
        key (str):
        target (str):
    """

    key: str
    target: str

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        target = self.target

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "key": key,
                "target": target,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        key = d.pop("key")

        target = d.pop("target")

        graph_resolver_config_aliases_item = cls(
            key=key,
            target=target,
        )

        return graph_resolver_config_aliases_item
