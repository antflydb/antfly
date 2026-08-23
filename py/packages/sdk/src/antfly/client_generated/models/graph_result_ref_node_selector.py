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
        result_ref (str): `$query_results` selects the final ranked query results. `$graph_results.<query-name>` selects
            a prior graph query result. Prior MATCH results require `binding`; traversal and path results prohibit it. A
            path result selects the endpoint node of each returned path.
        binding (str | Unset): Binding alias to select from a prior MATCH result. Valid only with
            `$graph_results.<query-name>` when that query returns MATCH rows.
        limit (int | Unset): Maximum referenced results to use. Omit only when the referenced result is complete.
    """

    result_ref: str
    binding: str | Unset = UNSET
    limit: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        result_ref = self.result_ref

        binding = self.binding

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "result_ref": result_ref,
            }
        )
        if binding is not UNSET:
            field_dict["binding"] = binding
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        result_ref = d.pop("result_ref")

        binding = d.pop("binding", UNSET)

        limit = d.pop("limit", UNSET)

        graph_result_ref_node_selector = cls(
            result_ref=result_ref,
            binding=binding,
            limit=limit,
        )

        return graph_result_ref_node_selector
