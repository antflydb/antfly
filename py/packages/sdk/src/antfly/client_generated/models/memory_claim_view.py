from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_record import MemoryRecord


T = TypeVar("T", bound="MemoryClaimView")


@_attrs_define
class MemoryClaimView:
    """
    Attributes:
        memory (MemoryRecord):
        fingerprint (str):
        config_generation (int):
    """

    memory: MemoryRecord
    fingerprint: str
    config_generation: int

    def to_dict(self) -> dict[str, Any]:
        memory = self.memory.to_dict()

        fingerprint = self.fingerprint

        config_generation = self.config_generation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "memory": memory,
                "fingerprint": fingerprint,
                "config_generation": config_generation,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_record import MemoryRecord

        d = dict(src_dict)
        memory = MemoryRecord.from_dict(d.pop("memory"))

        fingerprint = d.pop("fingerprint")

        config_generation = d.pop("config_generation")

        memory_claim_view = cls(
            memory=memory,
            fingerprint=fingerprint,
            config_generation=config_generation,
        )

        return memory_claim_view
