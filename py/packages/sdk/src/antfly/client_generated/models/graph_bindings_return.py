from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphBindingsReturn")


@_attrs_define
class GraphBindingsReturn:
    """
    Attributes:
        bindings (list[str]):
        limit (int | Unset):  Default: 100.
    """

    bindings: list[str]
    limit: int | Unset = 100

    def to_dict(self) -> dict[str, Any]:
        bindings = self.bindings

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "bindings": bindings,
            }
        )
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        bindings = cast(list[str], d.pop("bindings"))

        limit = d.pop("limit", UNSET)

        graph_bindings_return = cls(
            bindings=bindings,
            limit=limit,
        )

        return graph_bindings_return
