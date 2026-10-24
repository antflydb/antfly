from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_episode_turns_item_evidence import MemoryEpisodeTurnsItemEvidence


T = TypeVar("T", bound="MemoryEpisodeTurnsItem")


@_attrs_define
class MemoryEpisodeTurnsItem:
    """
    Attributes:
        evidence (MemoryEpisodeTurnsItemEvidence):
        entities (list[str]):
    """

    evidence: MemoryEpisodeTurnsItemEvidence
    entities: list[str]

    def to_dict(self) -> dict[str, Any]:
        evidence = self.evidence.to_dict()

        entities = self.entities

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "evidence": evidence,
                "entities": entities,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_episode_turns_item_evidence import MemoryEpisodeTurnsItemEvidence

        d = dict(src_dict)
        evidence = MemoryEpisodeTurnsItemEvidence.from_dict(d.pop("evidence"))

        entities = cast(list[str], d.pop("entities"))

        memory_episode_turns_item = cls(
            evidence=evidence,
            entities=entities,
        )

        return memory_episode_turns_item
