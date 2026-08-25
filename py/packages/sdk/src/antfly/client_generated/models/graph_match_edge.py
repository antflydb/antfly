from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphMatchEdge")


@_attrs_define
class GraphMatchEdge:
    """Outgoing structural edge expansion from the `from` alias to the `to` alias. Reverse a relationship by swapping those
    aliases; model an undirected relationship by indexing both directed edges. A fixed single-hop relationship preserves
    physical self-loops and may bind two distinct aliases to the same node identity. Variable-length expansion uses
    node-simple paths: a (table, key) identity is visited at most once within one expanded edge path, except when
    closing onto an already bound target alias for an explicit cycle. Exact distributed and serverless execution rejects
    planner-required reverse variable expansion when the source tables of unnamed intermediate nodes cannot be proven.
    Express cross-table multi-hop patterns as explicit single-hop edges with a table-qualified alias at each table
    boundary.

        Attributes:
            from_ (str): User-visible graph alias or named result under Antfly graph identifier policy v1 (Unicode 15.0.0).
                Identifiers are exact UTF-8 strings and are not normalized. Ordinary internal ASCII spaces are allowed. The
                value must not equal `*`, begin with `$`, have leading or trailing spaces, contain non-ASCII Unicode
                White_Space, or contain Unicode Cc control or Cf format code points. UTF-8 encoding is limited to 512 bytes.
            to (str): User-visible graph alias or named result under Antfly graph identifier policy v1 (Unicode 15.0.0).
                Identifiers are exact UTF-8 strings and are not normalized. Ordinary internal ASCII spaces are allowed. The
                value must not equal `*`, begin with `$`, have leading or trailing spaces, contain non-ASCII Unicode
                White_Space, or contain Unicode Cc control or Cf format code points. UTF-8 encoding is limited to 512 bytes.
            types (list[str] | Unset): Empty or omitted matches every edge type; otherwise at most 64 unique types totaling
                at most 64 KiB.
            min_hops (int | Unset):  Default: 1.
            max_hops (int | Unset):  Default: 1.
            min_weight (float | Unset):
            max_weight (float | Unset):
    """

    from_: str
    to: str
    types: list[str] | Unset = UNSET
    min_hops: int | Unset = 1
    max_hops: int | Unset = 1
    min_weight: float | Unset = UNSET
    max_weight: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from_ = self.from_

        to = self.to

        types: list[str] | Unset = UNSET
        if not isinstance(self.types, Unset):
            types = self.types

        min_hops = self.min_hops

        max_hops = self.max_hops

        min_weight = self.min_weight

        max_weight = self.max_weight

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "from": from_,
                "to": to,
            }
        )
        if types is not UNSET:
            field_dict["types"] = types
        if min_hops is not UNSET:
            field_dict["min_hops"] = min_hops
        if max_hops is not UNSET:
            field_dict["max_hops"] = max_hops
        if min_weight is not UNSET:
            field_dict["min_weight"] = min_weight
        if max_weight is not UNSET:
            field_dict["max_weight"] = max_weight

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        from_ = d.pop("from")

        to = d.pop("to")

        types = cast(list[str], d.pop("types", UNSET))

        min_hops = d.pop("min_hops", UNSET)

        max_hops = d.pop("max_hops", UNSET)

        min_weight = d.pop("min_weight", UNSET)

        max_weight = d.pop("max_weight", UNSET)

        graph_match_edge = cls(
            from_=from_,
            to=to,
            types=types,
            min_hops=min_hops,
            max_hops=max_hops,
            min_weight=min_weight,
            max_weight=max_weight,
        )

        return graph_match_edge
