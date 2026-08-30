from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphExactResultStats")


@_attrs_define
class GraphExactResultStats:
    """Completion statistics for a graph result that is exact or fails without producing a result.

    Attributes:
        returned_items (int): Number of primary result items returned (paths or aggregates).
        truncated (bool): Always false. Exact graph operations fail instead of returning partial output.
    """

    returned_items: int
    truncated: bool

    def to_dict(self) -> dict[str, Any]:
        returned_items = self.returned_items

        truncated = self.truncated

        field_dict: dict[str, Any] = {}

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

        graph_exact_result_stats = cls(
            returned_items=returned_items,
            truncated=truncated,
        )

        return graph_exact_result_stats
