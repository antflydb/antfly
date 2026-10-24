from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemoryLineageRevert")


@_attrs_define
class MemoryLineageRevert:
    """
    Attributes:
        checkpoint (str):
        expected_revision (int):
    """

    checkpoint: str
    expected_revision: int

    def to_dict(self) -> dict[str, Any]:
        checkpoint = self.checkpoint

        expected_revision = self.expected_revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "checkpoint": checkpoint,
                "expected_revision": expected_revision,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        checkpoint = d.pop("checkpoint")

        expected_revision = d.pop("expected_revision")

        memory_lineage_revert = cls(
            checkpoint=checkpoint,
            expected_revision=expected_revision,
        )

        return memory_lineage_revert
