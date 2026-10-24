from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_supersession_verdict_verdict import MemorySupersessionVerdictVerdict
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_supersession_verdict_evidence_item import MemorySupersessionVerdictEvidenceItem
    from ..models.memory_supersession_verdict_model_type_0 import MemorySupersessionVerdictModelType0


T = TypeVar("T", bound="MemorySupersessionVerdict")


@_attrs_define
class MemorySupersessionVerdict:
    """
    Attributes:
        expected_revision (int):
        verdict (MemorySupersessionVerdictVerdict | Unset):
        reason (str | Unset):
        actor (str | Unset):
        evidence (list[MemorySupersessionVerdictEvidenceItem] | Unset):
        model (MemorySupersessionVerdictModelType0 | None | Unset):
    """

    expected_revision: int
    verdict: MemorySupersessionVerdictVerdict | Unset = UNSET
    reason: str | Unset = UNSET
    actor: str | Unset = UNSET
    evidence: list[MemorySupersessionVerdictEvidenceItem] | Unset = UNSET
    model: MemorySupersessionVerdictModelType0 | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_supersession_verdict_model_type_0 import MemorySupersessionVerdictModelType0

        expected_revision = self.expected_revision

        verdict: str | Unset = UNSET
        if not isinstance(self.verdict, Unset):
            verdict = self.verdict.value

        reason = self.reason

        actor = self.actor

        evidence: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.evidence, Unset):
            evidence = []
            for evidence_item_data in self.evidence:
                evidence_item = evidence_item_data.to_dict()
                evidence.append(evidence_item)

        model: dict[str, Any] | None | Unset
        if isinstance(self.model, Unset):
            model = UNSET
        elif isinstance(self.model, MemorySupersessionVerdictModelType0):
            model = self.model.to_dict()
        else:
            model = self.model

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "expected_revision": expected_revision,
            }
        )
        if verdict is not UNSET:
            field_dict["verdict"] = verdict
        if reason is not UNSET:
            field_dict["reason"] = reason
        if actor is not UNSET:
            field_dict["actor"] = actor
        if evidence is not UNSET:
            field_dict["evidence"] = evidence
        if model is not UNSET:
            field_dict["model"] = model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_supersession_verdict_evidence_item import MemorySupersessionVerdictEvidenceItem
        from ..models.memory_supersession_verdict_model_type_0 import MemorySupersessionVerdictModelType0

        d = dict(src_dict)
        expected_revision = d.pop("expected_revision")

        _verdict = d.pop("verdict", UNSET)
        verdict: MemorySupersessionVerdictVerdict | Unset
        if isinstance(_verdict, Unset):
            verdict = UNSET
        else:
            verdict = MemorySupersessionVerdictVerdict(_verdict)

        reason = d.pop("reason", UNSET)

        actor = d.pop("actor", UNSET)

        _evidence = d.pop("evidence", UNSET)
        evidence: list[MemorySupersessionVerdictEvidenceItem] | Unset = UNSET
        if _evidence is not UNSET:
            evidence = []
            for evidence_item_data in _evidence:
                evidence_item = MemorySupersessionVerdictEvidenceItem.from_dict(evidence_item_data)

                evidence.append(evidence_item)

        def _parse_model(data: object) -> MemorySupersessionVerdictModelType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                model_type_0 = MemorySupersessionVerdictModelType0.from_dict(data)

                return model_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemorySupersessionVerdictModelType0 | None | Unset, data)

        model = _parse_model(d.pop("model", UNSET))

        memory_supersession_verdict = cls(
            expected_revision=expected_revision,
            verdict=verdict,
            reason=reason,
            actor=actor,
            evidence=evidence,
            model=model,
        )

        return memory_supersession_verdict
