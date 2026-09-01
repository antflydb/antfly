from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphPathEndpoint")


@_attrs_define
class GraphPathEndpoint:
    """
    Attributes:
        key (str):
        table (str | Unset): Optional table qualifier for an exact cross-table node identity. Omit for the query table.
    """

    key: str
    table: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        table = self.table

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "key": key,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        key = d.pop("key")

        table = d.pop("table", UNSET)

        graph_path_endpoint = cls(
            key=key,
            table=table,
        )

        return graph_path_endpoint
