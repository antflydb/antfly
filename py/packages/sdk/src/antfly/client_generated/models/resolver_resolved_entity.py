from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.resolver_resolved_entity_decision import ResolverResolvedEntityDecision
from ..models.resolver_resolved_entity_suppression_reason import ResolverResolvedEntitySuppressionReason
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_adjudication_evidence import ResolverAdjudicationEvidence
    from ..models.resolver_resolved_entity_doc_ref import ResolverResolvedEntityDocRef


T = TypeVar("T", bound="ResolverResolvedEntity")


@_attrs_define
class ResolverResolvedEntity:
    """
    Attributes:
        local_id (str):
        doc_ref (ResolverResolvedEntityDocRef):
        confidence (float):
        decision (ResolverResolvedEntityDecision):
        label (str | Unset):
        canonical_name (str | Unset):
        surface_form (str | Unset):
        rendered_key (str | Unset):
        redirected_from (str | Unset):
        suppressed (bool | Unset):
        suppression_reason (ResolverResolvedEntitySuppressionReason | Unset):
        identity_restored (bool | Unset):
        adjudication (ResolverAdjudicationEvidence | Unset):
    """

    local_id: str
    doc_ref: ResolverResolvedEntityDocRef
    confidence: float
    decision: ResolverResolvedEntityDecision
    label: str | Unset = UNSET
    canonical_name: str | Unset = UNSET
    surface_form: str | Unset = UNSET
    rendered_key: str | Unset = UNSET
    redirected_from: str | Unset = UNSET
    suppressed: bool | Unset = UNSET
    suppression_reason: ResolverResolvedEntitySuppressionReason | Unset = UNSET
    identity_restored: bool | Unset = UNSET
    adjudication: ResolverAdjudicationEvidence | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        local_id = self.local_id

        doc_ref = self.doc_ref.to_dict()

        confidence = self.confidence

        decision = self.decision.value

        label = self.label

        canonical_name = self.canonical_name

        surface_form = self.surface_form

        rendered_key = self.rendered_key

        redirected_from = self.redirected_from

        suppressed = self.suppressed

        suppression_reason: str | Unset = UNSET
        if not isinstance(self.suppression_reason, Unset):
            suppression_reason = self.suppression_reason.value

        identity_restored = self.identity_restored

        adjudication: dict[str, Any] | Unset = UNSET
        if not isinstance(self.adjudication, Unset):
            adjudication = self.adjudication.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "local_id": local_id,
                "doc_ref": doc_ref,
                "confidence": confidence,
                "decision": decision,
            }
        )
        if label is not UNSET:
            field_dict["label"] = label
        if canonical_name is not UNSET:
            field_dict["canonical_name"] = canonical_name
        if surface_form is not UNSET:
            field_dict["surface_form"] = surface_form
        if rendered_key is not UNSET:
            field_dict["rendered_key"] = rendered_key
        if redirected_from is not UNSET:
            field_dict["redirected_from"] = redirected_from
        if suppressed is not UNSET:
            field_dict["suppressed"] = suppressed
        if suppression_reason is not UNSET:
            field_dict["suppression_reason"] = suppression_reason
        if identity_restored is not UNSET:
            field_dict["identity_restored"] = identity_restored
        if adjudication is not UNSET:
            field_dict["adjudication"] = adjudication

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_adjudication_evidence import ResolverAdjudicationEvidence
        from ..models.resolver_resolved_entity_doc_ref import ResolverResolvedEntityDocRef

        d = dict(src_dict)
        local_id = d.pop("local_id")

        doc_ref = ResolverResolvedEntityDocRef.from_dict(d.pop("doc_ref"))

        confidence = d.pop("confidence")

        decision = ResolverResolvedEntityDecision(d.pop("decision"))

        label = d.pop("label", UNSET)

        canonical_name = d.pop("canonical_name", UNSET)

        surface_form = d.pop("surface_form", UNSET)

        rendered_key = d.pop("rendered_key", UNSET)

        redirected_from = d.pop("redirected_from", UNSET)

        suppressed = d.pop("suppressed", UNSET)

        _suppression_reason = d.pop("suppression_reason", UNSET)
        suppression_reason: ResolverResolvedEntitySuppressionReason | Unset
        if isinstance(_suppression_reason, Unset):
            suppression_reason = UNSET
        else:
            suppression_reason = ResolverResolvedEntitySuppressionReason(_suppression_reason)

        identity_restored = d.pop("identity_restored", UNSET)

        _adjudication = d.pop("adjudication", UNSET)
        adjudication: ResolverAdjudicationEvidence | Unset
        if isinstance(_adjudication, Unset):
            adjudication = UNSET
        else:
            adjudication = ResolverAdjudicationEvidence.from_dict(_adjudication)

        resolver_resolved_entity = cls(
            local_id=local_id,
            doc_ref=doc_ref,
            confidence=confidence,
            decision=decision,
            label=label,
            canonical_name=canonical_name,
            surface_form=surface_form,
            rendered_key=rendered_key,
            redirected_from=redirected_from,
            suppressed=suppressed,
            suppression_reason=suppression_reason,
            identity_restored=identity_restored,
            adjudication=adjudication,
        )

        resolver_resolved_entity.additional_properties = d
        return resolver_resolved_entity

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
