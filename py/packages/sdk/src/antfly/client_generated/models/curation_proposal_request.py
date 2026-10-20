from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.curation_proposal_request_action import CurationProposalRequestAction
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.curation_proposal_options import CurationProposalOptions
    from ..models.curation_proposal_request_evidence_type_0 import CurationProposalRequestEvidenceType0


T = TypeVar("T", bound="CurationProposalRequest")


@_attrs_define
class CurationProposalRequest:
    """
    Attributes:
        action (CurationProposalRequestAction):
        key (str):
        into (None | str | Unset):
        options (CurationProposalOptions | Unset):
        confidence (float | None | Unset):
        evidence (CurationProposalRequestEvidenceType0 | None | Unset): At most 16 KiB, 4096 values and depth 32.
        actor (str | Unset):
        reason (str | Unset):
    """

    action: CurationProposalRequestAction
    key: str
    into: None | str | Unset = UNSET
    options: CurationProposalOptions | Unset = UNSET
    confidence: float | None | Unset = UNSET
    evidence: CurationProposalRequestEvidenceType0 | None | Unset = UNSET
    actor: str | Unset = UNSET
    reason: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.curation_proposal_request_evidence_type_0 import CurationProposalRequestEvidenceType0

        action = self.action.value

        key = self.key

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
        elif isinstance(self.evidence, CurationProposalRequestEvidenceType0):
            evidence = self.evidence.to_dict()
        else:
            evidence = self.evidence

        actor = self.actor

        reason = self.reason

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "action": action,
                "key": key,
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
        if actor is not UNSET:
            field_dict["actor"] = actor
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.curation_proposal_options import CurationProposalOptions
        from ..models.curation_proposal_request_evidence_type_0 import CurationProposalRequestEvidenceType0

        d = dict(src_dict)
        action = CurationProposalRequestAction(d.pop("action"))

        key = d.pop("key")

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

        def _parse_evidence(data: object) -> CurationProposalRequestEvidenceType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                evidence_type_0 = CurationProposalRequestEvidenceType0.from_dict(data)

                return evidence_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(CurationProposalRequestEvidenceType0 | None | Unset, data)

        evidence = _parse_evidence(d.pop("evidence", UNSET))

        actor = d.pop("actor", UNSET)

        reason = d.pop("reason", UNSET)

        curation_proposal_request = cls(
            action=action,
            key=key,
            into=into,
            options=options,
            confidence=confidence,
            evidence=evidence,
            actor=actor,
            reason=reason,
        )

        return curation_proposal_request
