from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphAliasCountAggregate")


@_attrs_define
class GraphAliasCountAggregate:
    """
    Attributes:
        count (str): User-visible graph alias or named result under Antfly graph identifier policy v1 (Unicode 15.0.0).
            Identifiers are exact UTF-8 strings and are not normalized. Ordinary internal ASCII spaces are allowed. The
            value must not equal `*`, begin with `$`, have leading or trailing spaces, contain non-ASCII Unicode
            White_Space, or contain Unicode Cc control or Cf format code points. UTF-8 encoding is limited to 512 bytes.
        distinct (bool | Unset): Count exact table-qualified identities. Exact distinct sets share a request resource
            budget and fail with `graph_distinct_budget_exceeded` instead of returning a partial count. Default: False.
    """

    count: str
    distinct: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        count = self.count

        distinct = self.distinct

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "count": count,
            }
        )
        if distinct is not UNSET:
            field_dict["distinct"] = distinct

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        count = d.pop("count")

        distinct = d.pop("distinct", UNSET)

        graph_alias_count_aggregate = cls(
            count=count,
            distinct=distinct,
        )

        return graph_alias_count_aggregate
