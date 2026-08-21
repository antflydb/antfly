from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphPathEndpoint")


@_attrs_define
class GraphPathEndpoint:
    """
    Attributes:
        key (str):
    """

    key: str

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "key": key,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        key = d.pop("key")

        graph_path_endpoint = cls(
            key=key,
        )

        return graph_path_endpoint
