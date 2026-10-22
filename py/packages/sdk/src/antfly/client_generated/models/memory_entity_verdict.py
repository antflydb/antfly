from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity_verdict_evidence_item import MemoryEntityVerdictEvidenceItem
    from ..models.memory_entity_verdict_model_type_0 import MemoryEntityVerdictModelType0
    from ..models.memory_entity_verdict_views_item import MemoryEntityVerdictViewsItem


T = TypeVar("T", bound="MemoryEntityVerdict")


@_attrs_define
class MemoryEntityVerdict:
    """
    Attributes:
        expected_revision (int):
        views (list[MemoryEntityVerdictViewsItem] | Unset):
        accept (bool | None | Unset):
        pin_distinct (bool | Unset):  Default: False.
        actor (str | Unset):
        reason (str | Unset):
        evidence (list[MemoryEntityVerdictEvidenceItem] | Unset):
        model (MemoryEntityVerdictModelType0 | None | Unset):
    """

    expected_revision: int
    views: list[MemoryEntityVerdictViewsItem] | Unset = UNSET
    accept: bool | None | Unset = UNSET
    pin_distinct: bool | Unset = False
    actor: str | Unset = UNSET
    reason: str | Unset = UNSET
    evidence: list[MemoryEntityVerdictEvidenceItem] | Unset = UNSET
    model: MemoryEntityVerdictModelType0 | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_entity_verdict_model_type_0 import MemoryEntityVerdictModelType0

        expected_revision = self.expected_revision

        views: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.views, Unset):
            views = []
            for views_item_data in self.views:
                views_item = views_item_data.to_dict()
                views.append(views_item)

        accept: bool | None | Unset
        if isinstance(self.accept, Unset):
            accept = UNSET
        else:
            accept = self.accept

        pin_distinct = self.pin_distinct

        actor = self.actor

        reason = self.reason

        evidence: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.evidence, Unset):
            evidence = []
            for evidence_item_data in self.evidence:
                evidence_item = evidence_item_data.to_dict()
                evidence.append(evidence_item)

        model: dict[str, Any] | None | Unset
        if isinstance(self.model, Unset):
            model = UNSET
        elif isinstance(self.model, MemoryEntityVerdictModelType0):
            model = self.model.to_dict()
        else:
            model = self.model

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "expected_revision": expected_revision,
            }
        )
        if views is not UNSET:
            field_dict["views"] = views
        if accept is not UNSET:
            field_dict["accept"] = accept
        if pin_distinct is not UNSET:
            field_dict["pin_distinct"] = pin_distinct
        if actor is not UNSET:
            field_dict["actor"] = actor
        if reason is not UNSET:
            field_dict["reason"] = reason
        if evidence is not UNSET:
            field_dict["evidence"] = evidence
        if model is not UNSET:
            field_dict["model"] = model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_verdict_evidence_item import MemoryEntityVerdictEvidenceItem
        from ..models.memory_entity_verdict_model_type_0 import MemoryEntityVerdictModelType0
        from ..models.memory_entity_verdict_views_item import MemoryEntityVerdictViewsItem

        d = dict(src_dict)
        expected_revision = d.pop("expected_revision")

        _views = d.pop("views", UNSET)
        views: list[MemoryEntityVerdictViewsItem] | Unset = UNSET
        if _views is not UNSET:
            views = []
            for views_item_data in _views:
                views_item = MemoryEntityVerdictViewsItem.from_dict(views_item_data)

                views.append(views_item)

        def _parse_accept(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        accept = _parse_accept(d.pop("accept", UNSET))

        pin_distinct = d.pop("pin_distinct", UNSET)

        actor = d.pop("actor", UNSET)

        reason = d.pop("reason", UNSET)

        _evidence = d.pop("evidence", UNSET)
        evidence: list[MemoryEntityVerdictEvidenceItem] | Unset = UNSET
        if _evidence is not UNSET:
            evidence = []
            for evidence_item_data in _evidence:
                evidence_item = MemoryEntityVerdictEvidenceItem.from_dict(evidence_item_data)

                evidence.append(evidence_item)

        def _parse_model(data: object) -> MemoryEntityVerdictModelType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                model_type_0 = MemoryEntityVerdictModelType0.from_dict(data)

                return model_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEntityVerdictModelType0 | None | Unset, data)

        model = _parse_model(d.pop("model", UNSET))

        memory_entity_verdict = cls(
            expected_revision=expected_revision,
            views=views,
            accept=accept,
            pin_distinct=pin_distinct,
            actor=actor,
            reason=reason,
            evidence=evidence,
            model=model,
        )

        return memory_entity_verdict
