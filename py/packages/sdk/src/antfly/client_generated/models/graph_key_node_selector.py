from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphKeyNodeSelector")


@_attrs_define
class GraphKeyNodeSelector:
    """
    Attributes:
        keys (list[str]): Exact keys in the table being queried.
    """

    keys: list[str]

    def to_dict(self) -> dict[str, Any]:
        keys = self.keys

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "keys": keys,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        keys = cast(list[str], d.pop("keys"))

        graph_key_node_selector = cls(
            keys=keys,
        )

        return graph_key_node_selector
