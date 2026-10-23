from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_recall_result_episodes_item_turns_item_evidence_materialization_type_0_screening import (
        MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0Screening,
    )


T = TypeVar("T", bound="MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0")


@_attrs_define
class MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0:
    """
    Attributes:
        screening (MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0Screening):
        text_redacted (bool):
        text_sha256 (str):
    """

    screening: MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0Screening
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
        from ..models.memory_recall_result_episodes_item_turns_item_evidence_materialization_type_0_screening import (
            MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0Screening,
        )

        d = dict(src_dict)
        screening = MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0Screening.from_dict(
            d.pop("screening")
        )

        text_redacted = d.pop("text_redacted")

        text_sha256 = d.pop("text_sha256")

        memory_recall_result_episodes_item_turns_item_evidence_materialization_type_0 = cls(
            screening=screening,
            text_redacted=text_redacted,
            text_sha256=text_sha256,
        )

        return memory_recall_result_episodes_item_turns_item_evidence_materialization_type_0
