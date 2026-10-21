from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.provenance_decision_origin_type_0_approver_type_0_assurance import (
    ProvenanceDecisionOriginType0ApproverType0Assurance,
)
from ..models.provenance_decision_origin_type_0_approver_type_0_kind import (
    ProvenanceDecisionOriginType0ApproverType0Kind,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ProvenanceDecisionOriginType0ApproverType0")


@_attrs_define
class ProvenanceDecisionOriginType0ApproverType0:
    """
    Attributes:
        id (str):
        kind (ProvenanceDecisionOriginType0ApproverType0Kind | Unset):
        assurance (ProvenanceDecisionOriginType0ApproverType0Assurance | Unset):
    """

    id: str
    kind: ProvenanceDecisionOriginType0ApproverType0Kind | Unset = UNSET
    assurance: ProvenanceDecisionOriginType0ApproverType0Assurance | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        assurance: str | Unset = UNSET
        if not isinstance(self.assurance, Unset):
            assurance = self.assurance.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
            }
        )
        if kind is not UNSET:
            field_dict["kind"] = kind
        if assurance is not UNSET:
            field_dict["assurance"] = assurance

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        _kind = d.pop("kind", UNSET)
        kind: ProvenanceDecisionOriginType0ApproverType0Kind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = ProvenanceDecisionOriginType0ApproverType0Kind(_kind)

        _assurance = d.pop("assurance", UNSET)
        assurance: ProvenanceDecisionOriginType0ApproverType0Assurance | Unset
        if isinstance(_assurance, Unset):
            assurance = UNSET
        else:
            assurance = ProvenanceDecisionOriginType0ApproverType0Assurance(_assurance)

        provenance_decision_origin_type_0_approver_type_0 = cls(
            id=id,
            kind=kind,
            assurance=assurance,
        )

        return provenance_decision_origin_type_0_approver_type_0
