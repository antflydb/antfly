from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemoryLineageCheckpointRequest")


@_attrs_define
class MemoryLineageCheckpointRequest:
    """
    Attributes:
        id (str):
        expected_revision (int):
    """

    id: str
    expected_revision: int

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        expected_revision = self.expected_revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "expected_revision": expected_revision,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        expected_revision = d.pop("expected_revision")

        memory_lineage_checkpoint_request = cls(
            id=id,
            expected_revision=expected_revision,
        )

        return memory_lineage_checkpoint_request
