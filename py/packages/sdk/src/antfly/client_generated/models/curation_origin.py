from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.curation_origin_origin import CurationOriginOrigin
from ..types import UNSET, Unset

T = TypeVar("T", bound="CurationOrigin")


@_attrs_define
class CurationOrigin:
    """Server-owned provenance. Clients supply actor and reason on writes. Zero time/revision denote legacy records whose
    creation is unknown.

        Attributes:
            origin (CurationOriginOrigin | Unset):
            actor (str | Unset): UTF-8 client attribution, limited to 256 bytes.
            reason (str | Unset): UTF-8 explanation, limited to 4096 bytes.
            created_at_ms (int | Unset):
            revision (int | Unset):
            proposal_id (None | str | Unset):
            cascade_from (None | str | Unset):
    """

    origin: CurationOriginOrigin | Unset = UNSET
    actor: str | Unset = UNSET
    reason: str | Unset = UNSET
    created_at_ms: int | Unset = UNSET
    revision: int | Unset = UNSET
    proposal_id: None | str | Unset = UNSET
    cascade_from: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        origin: str | Unset = UNSET
        if not isinstance(self.origin, Unset):
            origin = self.origin.value

        actor = self.actor

        reason = self.reason

        created_at_ms = self.created_at_ms

        revision = self.revision

        proposal_id: None | str | Unset
        if isinstance(self.proposal_id, Unset):
            proposal_id = UNSET
        else:
            proposal_id = self.proposal_id

        cascade_from: None | str | Unset
        if isinstance(self.cascade_from, Unset):
            cascade_from = UNSET
        else:
            cascade_from = self.cascade_from

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if origin is not UNSET:
            field_dict["origin"] = origin
        if actor is not UNSET:
            field_dict["actor"] = actor
        if reason is not UNSET:
            field_dict["reason"] = reason
        if created_at_ms is not UNSET:
            field_dict["created_at_ms"] = created_at_ms
        if revision is not UNSET:
            field_dict["revision"] = revision
        if proposal_id is not UNSET:
            field_dict["proposal_id"] = proposal_id
        if cascade_from is not UNSET:
            field_dict["cascade_from"] = cascade_from

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _origin = d.pop("origin", UNSET)
        origin: CurationOriginOrigin | Unset
        if isinstance(_origin, Unset):
            origin = UNSET
        else:
            origin = CurationOriginOrigin(_origin)

        actor = d.pop("actor", UNSET)

        reason = d.pop("reason", UNSET)

        created_at_ms = d.pop("created_at_ms", UNSET)

        revision = d.pop("revision", UNSET)

        def _parse_proposal_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        proposal_id = _parse_proposal_id(d.pop("proposal_id", UNSET))

        def _parse_cascade_from(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        cascade_from = _parse_cascade_from(d.pop("cascade_from", UNSET))

        curation_origin = cls(
            origin=origin,
            actor=actor,
            reason=reason,
            created_at_ms=created_at_ms,
            revision=revision,
            proposal_id=proposal_id,
            cascade_from=cascade_from,
        )

        curation_origin.additional_properties = d
        return curation_origin

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
