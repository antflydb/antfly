from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_recall_result_episodes_item_turns_item_evidence_evidence import (
        MemoryRecallResultEpisodesItemTurnsItemEvidenceEvidence,
    )
    from ..models.memory_recall_result_episodes_item_turns_item_evidence_materialization_type_0 import (
        MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0,
    )


T = TypeVar("T", bound="MemoryRecallResultEpisodesItemTurnsItemEvidence")


@_attrs_define
class MemoryRecallResultEpisodesItemTurnsItemEvidence:
    """
    Attributes:
        evidence (MemoryRecallResultEpisodesItemTurnsItemEvidenceEvidence):
        owner (str):
        recorded_at (int):
        revision (int):
        materialization (MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0 | None | Unset):
        context_only (bool | Unset):  Default: False.
        forgotten (bool | Unset):  Default: False.
    """

    evidence: MemoryRecallResultEpisodesItemTurnsItemEvidenceEvidence
    owner: str
    recorded_at: int
    revision: int
    materialization: MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0 | None | Unset = UNSET
    context_only: bool | Unset = False
    forgotten: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_recall_result_episodes_item_turns_item_evidence_materialization_type_0 import (
            MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0,
        )

        evidence = self.evidence.to_dict()

        owner = self.owner

        recorded_at = self.recorded_at

        revision = self.revision

        materialization: dict[str, Any] | None | Unset
        if isinstance(self.materialization, Unset):
            materialization = UNSET
        elif isinstance(self.materialization, MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0):
            materialization = self.materialization.to_dict()
        else:
            materialization = self.materialization

        context_only = self.context_only

        forgotten = self.forgotten

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "evidence": evidence,
                "owner": owner,
                "recorded_at": recorded_at,
                "revision": revision,
            }
        )
        if materialization is not UNSET:
            field_dict["materialization"] = materialization
        if context_only is not UNSET:
            field_dict["context_only"] = context_only
        if forgotten is not UNSET:
            field_dict["forgotten"] = forgotten

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_recall_result_episodes_item_turns_item_evidence_evidence import (
            MemoryRecallResultEpisodesItemTurnsItemEvidenceEvidence,
        )
        from ..models.memory_recall_result_episodes_item_turns_item_evidence_materialization_type_0 import (
            MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0,
        )

        d = dict(src_dict)
        evidence = MemoryRecallResultEpisodesItemTurnsItemEvidenceEvidence.from_dict(d.pop("evidence"))

        owner = d.pop("owner")

        recorded_at = d.pop("recorded_at")

        revision = d.pop("revision")

        def _parse_materialization(
            data: object,
        ) -> MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                materialization_type_0 = MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0.from_dict(
                    data
                )

                return materialization_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryRecallResultEpisodesItemTurnsItemEvidenceMaterializationType0 | None | Unset, data)

        materialization = _parse_materialization(d.pop("materialization", UNSET))

        context_only = d.pop("context_only", UNSET)

        forgotten = d.pop("forgotten", UNSET)

        memory_recall_result_episodes_item_turns_item_evidence = cls(
            evidence=evidence,
            owner=owner,
            recorded_at=recorded_at,
            revision=revision,
            materialization=materialization,
            context_only=context_only,
            forgotten=forgotten,
        )

        return memory_recall_result_episodes_item_turns_item_evidence
