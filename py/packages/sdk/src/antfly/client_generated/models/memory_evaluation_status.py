from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_evaluation_report import MemoryEvaluationReport
    from ..models.memory_evaluation_run import MemoryEvaluationRun


T = TypeVar("T", bound="MemoryEvaluationStatus")


@_attrs_define
class MemoryEvaluationStatus:
    """
    Attributes:
        run (MemoryEvaluationRun):
        identity (MemoryEvaluationReport):
        relationship (MemoryEvaluationReport):
        supersession (MemoryEvaluationReport):
        retrieval (MemoryEvaluationReport):
        identity_admission (MemoryEvaluationReport):
        relationship_admission (MemoryEvaluationReport):
        supersession_admission (MemoryEvaluationReport):
    """

    run: MemoryEvaluationRun
    identity: MemoryEvaluationReport
    relationship: MemoryEvaluationReport
    supersession: MemoryEvaluationReport
    retrieval: MemoryEvaluationReport
    identity_admission: MemoryEvaluationReport
    relationship_admission: MemoryEvaluationReport
    supersession_admission: MemoryEvaluationReport

    def to_dict(self) -> dict[str, Any]:
        run = self.run.to_dict()

        identity = self.identity.to_dict()

        relationship = self.relationship.to_dict()

        supersession = self.supersession.to_dict()

        retrieval = self.retrieval.to_dict()

        identity_admission = self.identity_admission.to_dict()

        relationship_admission = self.relationship_admission.to_dict()

        supersession_admission = self.supersession_admission.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "run": run,
                "identity": identity,
                "relationship": relationship,
                "supersession": supersession,
                "retrieval": retrieval,
                "identity_admission": identity_admission,
                "relationship_admission": relationship_admission,
                "supersession_admission": supersession_admission,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_report import MemoryEvaluationReport
        from ..models.memory_evaluation_run import MemoryEvaluationRun

        d = dict(src_dict)
        run = MemoryEvaluationRun.from_dict(d.pop("run"))

        identity = MemoryEvaluationReport.from_dict(d.pop("identity"))

        relationship = MemoryEvaluationReport.from_dict(d.pop("relationship"))

        supersession = MemoryEvaluationReport.from_dict(d.pop("supersession"))

        retrieval = MemoryEvaluationReport.from_dict(d.pop("retrieval"))

        identity_admission = MemoryEvaluationReport.from_dict(d.pop("identity_admission"))

        relationship_admission = MemoryEvaluationReport.from_dict(d.pop("relationship_admission"))

        supersession_admission = MemoryEvaluationReport.from_dict(d.pop("supersession_admission"))

        memory_evaluation_status = cls(
            run=run,
            identity=identity,
            relationship=relationship,
            supersession=supersession,
            retrieval=retrieval,
            identity_admission=identity_admission,
            relationship_admission=relationship_admission,
            supersession_admission=supersession_admission,
        )

        return memory_evaluation_status
