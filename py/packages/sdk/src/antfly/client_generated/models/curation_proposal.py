from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.curation_proposal_action import CurationProposalAction
from ..models.curation_proposal_state import CurationProposalState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.curation_origin import CurationOrigin
    from ..models.curation_proposal_evidence_type_0 import CurationProposalEvidenceType0
    from ..models.curation_proposal_options import CurationProposalOptions


T = TypeVar("T", bound="CurationProposal")


@_attrs_define
class CurationProposal:
    """
    Attributes:
        id (str):
        action (CurationProposalAction):
        key (str):
        origin (CurationOrigin): Server-owned provenance. Clients supply actor and reason on writes. Zero time/revision
            denote legacy records whose creation is unknown.
        state (CurationProposalState):
        into (None | str | Unset):
        options (CurationProposalOptions | Unset):
        confidence (float | None | Unset):
        evidence (CurationProposalEvidenceType0 | None | Unset):
        decided (CurationOrigin | None | Unset):
    """

    id: str
    action: CurationProposalAction
    key: str
    origin: CurationOrigin
    state: CurationProposalState
    into: None | str | Unset = UNSET
    options: CurationProposalOptions | Unset = UNSET
    confidence: float | None | Unset = UNSET
    evidence: CurationProposalEvidenceType0 | None | Unset = UNSET
    decided: CurationOrigin | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.curation_origin import CurationOrigin
        from ..models.curation_proposal_evidence_type_0 import CurationProposalEvidenceType0

        id = self.id

        action = self.action.value

        key = self.key

        origin = self.origin.to_dict()

        state = self.state.value

        into: None | str | Unset
        if isinstance(self.into, Unset):
            into = UNSET
        else:
            into = self.into

        options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.options, Unset):
            options = self.options.to_dict()

        confidence: float | None | Unset
        if isinstance(self.confidence, Unset):
            confidence = UNSET
        else:
            confidence = self.confidence

        evidence: dict[str, Any] | None | Unset
        if isinstance(self.evidence, Unset):
            evidence = UNSET
        elif isinstance(self.evidence, CurationProposalEvidenceType0):
            evidence = self.evidence.to_dict()
        else:
            evidence = self.evidence

        decided: dict[str, Any] | None | Unset
        if isinstance(self.decided, Unset):
            decided = UNSET
        elif isinstance(self.decided, CurationOrigin):
            decided = self.decided.to_dict()
        else:
            decided = self.decided

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "action": action,
                "key": key,
                "origin": origin,
                "state": state,
            }
        )
        if into is not UNSET:
            field_dict["into"] = into
        if options is not UNSET:
            field_dict["options"] = options
        if confidence is not UNSET:
            field_dict["confidence"] = confidence
        if evidence is not UNSET:
            field_dict["evidence"] = evidence
        if decided is not UNSET:
            field_dict["decided"] = decided

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.curation_origin import CurationOrigin
        from ..models.curation_proposal_evidence_type_0 import CurationProposalEvidenceType0
        from ..models.curation_proposal_options import CurationProposalOptions

        d = dict(src_dict)
        id = d.pop("id")

        action = CurationProposalAction(d.pop("action"))

        key = d.pop("key")

        origin = CurationOrigin.from_dict(d.pop("origin"))

        state = CurationProposalState(d.pop("state"))

        def _parse_into(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        into = _parse_into(d.pop("into", UNSET))

        _options = d.pop("options", UNSET)
        options: CurationProposalOptions | Unset
        if isinstance(_options, Unset):
            options = UNSET
        else:
            options = CurationProposalOptions.from_dict(_options)

        def _parse_confidence(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        confidence = _parse_confidence(d.pop("confidence", UNSET))

        def _parse_evidence(data: object) -> CurationProposalEvidenceType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                evidence_type_0 = CurationProposalEvidenceType0.from_dict(data)

                return evidence_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(CurationProposalEvidenceType0 | None | Unset, data)

        evidence = _parse_evidence(d.pop("evidence", UNSET))

        def _parse_decided(data: object) -> CurationOrigin | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                decided_type_1 = CurationOrigin.from_dict(data)

                return decided_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(CurationOrigin | None | Unset, data)

        decided = _parse_decided(d.pop("decided", UNSET))

        curation_proposal = cls(
            id=id,
            action=action,
            key=key,
            origin=origin,
            state=state,
            into=into,
            options=options,
            confidence=confidence,
            evidence=evidence,
            decided=decided,
        )

        curation_proposal.additional_properties = d
        return curation_proposal

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
