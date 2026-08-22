from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphResultRefNodeSelector")


@_attrs_define
class GraphResultRefNodeSelector:
    """
    Attributes:
        result_ref (str): A prior result set: `$full_text_results`, `$embeddings_results`, `$fused_results`, or
            `$graph_results.<query-name>`.
        limit (int | Unset): Maximum referenced results to use. Omit only when the referenced result is complete.
    """

    result_ref: str
    limit: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        result_ref = self.result_ref

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "result_ref": result_ref,
            }
        )
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        result_ref = d.pop("result_ref")

        limit = d.pop("limit", UNSET)

        graph_result_ref_node_selector = cls(
            result_ref=result_ref,
            limit=limit,
        )

        return graph_result_ref_node_selector
