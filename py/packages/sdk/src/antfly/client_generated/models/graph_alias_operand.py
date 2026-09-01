from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphAliasOperand")


@_attrs_define
class GraphAliasOperand:
    """
    Attributes:
        alias (str): User-visible graph alias or named result under Antfly graph identifier policy v1 (Unicode 15.0.0).
            Identifiers are exact UTF-8 strings and are not normalized. Ordinary internal ASCII spaces are allowed. The
            value must not equal `*`, begin with `$`, have leading or trailing spaces, contain non-ASCII Unicode
            White_Space, or contain Unicode Cc control or Cf format code points. UTF-8 encoding is limited to 512 bytes.
    """

    alias: str

    def to_dict(self) -> dict[str, Any]:
        alias = self.alias

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "alias": alias,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        alias = d.pop("alias")

        graph_alias_operand = cls(
            alias=alias,
        )

        return graph_alias_operand
