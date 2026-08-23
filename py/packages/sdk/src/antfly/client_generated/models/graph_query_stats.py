from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="GraphQueryStats")


@_attrs_define
class GraphQueryStats:
    """
    Attributes:
        returned_items (int): Number of primary result items returned (nodes, paths, rows, or aggregates).
        truncated (bool): True when execution stopped before exhaustive enumeration; an unbounded result reference
            rejects truncated input.
    """

    returned_items: int
    truncated: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        returned_items = self.returned_items

        truncated = self.truncated

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "returned_items": returned_items,
                "truncated": truncated,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        returned_items = d.pop("returned_items")

        truncated = d.pop("truncated")

        graph_query_stats = cls(
            returned_items=returned_items,
            truncated=truncated,
        )

        graph_query_stats.additional_properties = d
        return graph_query_stats

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
