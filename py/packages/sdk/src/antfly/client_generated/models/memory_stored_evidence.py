from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_evidence import MemoryEvidence
    from ..models.memory_intake_materialization import MemoryIntakeMaterialization


T = TypeVar("T", bound="MemoryStoredEvidence")


@_attrs_define
class MemoryStoredEvidence:
    """
    Attributes:
        evidence (MemoryEvidence):
        owner (str):
        recorded_at (int):
        revision (int):
        materialization (MemoryIntakeMaterialization | Unset):
        context_only (bool | Unset):  Default: False.
        forgotten (bool | Unset):  Default: False.
    """

    evidence: MemoryEvidence
    owner: str
    recorded_at: int
    revision: int
    materialization: MemoryIntakeMaterialization | Unset = UNSET
    context_only: bool | Unset = False
    forgotten: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        evidence = self.evidence.to_dict()

        owner = self.owner

        recorded_at = self.recorded_at

        revision = self.revision

        materialization: dict[str, Any] | Unset = UNSET
        if not isinstance(self.materialization, Unset):
            materialization = self.materialization.to_dict()

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
        from ..models.memory_evidence import MemoryEvidence
        from ..models.memory_intake_materialization import MemoryIntakeMaterialization

        d = dict(src_dict)
        evidence = MemoryEvidence.from_dict(d.pop("evidence"))

        owner = d.pop("owner")

        recorded_at = d.pop("recorded_at")

        revision = d.pop("revision")

        _materialization = d.pop("materialization", UNSET)
        materialization: MemoryIntakeMaterialization | Unset
        if isinstance(_materialization, Unset):
            materialization = UNSET
        else:
            materialization = MemoryIntakeMaterialization.from_dict(_materialization)

        context_only = d.pop("context_only", UNSET)

        forgotten = d.pop("forgotten", UNSET)

        memory_stored_evidence = cls(
            evidence=evidence,
            owner=owner,
            recorded_at=recorded_at,
            revision=revision,
            materialization=materialization,
            context_only=context_only,
            forgotten=forgotten,
        )

        return memory_stored_evidence
