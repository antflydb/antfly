from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_intake_materialization_screening import MemoryIntakeMaterializationScreening


T = TypeVar("T", bound="MemoryIntakeMaterialization")


@_attrs_define
class MemoryIntakeMaterialization:
    """
    Attributes:
        screening (MemoryIntakeMaterializationScreening):
        text_redacted (bool):
        text_sha256 (str):
    """

    screening: MemoryIntakeMaterializationScreening
    text_redacted: bool
    text_sha256: str

    def to_dict(self) -> dict[str, Any]:
        screening = self.screening.to_dict()

        text_redacted = self.text_redacted

        text_sha256 = self.text_sha256

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "screening": screening,
                "text_redacted": text_redacted,
                "text_sha256": text_sha256,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_intake_materialization_screening import MemoryIntakeMaterializationScreening

        d = dict(src_dict)
        screening = MemoryIntakeMaterializationScreening.from_dict(d.pop("screening"))

        text_redacted = d.pop("text_redacted")

        text_sha256 = d.pop("text_sha256")

        memory_intake_materialization = cls(
            screening=screening,
            text_redacted=text_redacted,
            text_sha256=text_sha256,
        )

        return memory_intake_materialization
