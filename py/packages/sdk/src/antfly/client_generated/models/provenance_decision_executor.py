from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.provenance_decision_executor_assurance import ProvenanceDecisionExecutorAssurance
from ..models.provenance_decision_executor_kind import ProvenanceDecisionExecutorKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="ProvenanceDecisionExecutor")


@_attrs_define
class ProvenanceDecisionExecutor:
    """
    Attributes:
        id (str):
        kind (ProvenanceDecisionExecutorKind | Unset):
        assurance (ProvenanceDecisionExecutorAssurance | Unset):
    """

    id: str
    kind: ProvenanceDecisionExecutorKind | Unset = UNSET
    assurance: ProvenanceDecisionExecutorAssurance | Unset = UNSET

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
        kind: ProvenanceDecisionExecutorKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = ProvenanceDecisionExecutorKind(_kind)

        _assurance = d.pop("assurance", UNSET)
        assurance: ProvenanceDecisionExecutorAssurance | Unset
        if isinstance(_assurance, Unset):
            assurance = UNSET
        else:
            assurance = ProvenanceDecisionExecutorAssurance(_assurance)

        provenance_decision_executor = cls(
            id=id,
            kind=kind,
            assurance=assurance,
        )

        return provenance_decision_executor
