from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.provenance_origin_approver_type_0_assurance import ProvenanceOriginApproverType0Assurance
from ..models.provenance_origin_approver_type_0_kind import ProvenanceOriginApproverType0Kind
from ..types import UNSET, Unset

T = TypeVar("T", bound="ProvenanceOriginApproverType0")


@_attrs_define
class ProvenanceOriginApproverType0:
    """
    Attributes:
        id (str):
        kind (ProvenanceOriginApproverType0Kind | Unset):  Default: ProvenanceOriginApproverType0Kind.USER.
        assurance (ProvenanceOriginApproverType0Assurance | Unset):  Default:
            ProvenanceOriginApproverType0Assurance.DECLARED.
    """

    id: str
    kind: ProvenanceOriginApproverType0Kind | Unset = ProvenanceOriginApproverType0Kind.USER
    assurance: ProvenanceOriginApproverType0Assurance | Unset = ProvenanceOriginApproverType0Assurance.DECLARED

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
        kind: ProvenanceOriginApproverType0Kind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = ProvenanceOriginApproverType0Kind(_kind)

        _assurance = d.pop("assurance", UNSET)
        assurance: ProvenanceOriginApproverType0Assurance | Unset
        if isinstance(_assurance, Unset):
            assurance = UNSET
        else:
            assurance = ProvenanceOriginApproverType0Assurance(_assurance)

        provenance_origin_approver_type_0 = cls(
            id=id,
            kind=kind,
            assurance=assurance,
        )

        return provenance_origin_approver_type_0
