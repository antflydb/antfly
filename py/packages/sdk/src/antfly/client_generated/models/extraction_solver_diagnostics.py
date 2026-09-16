from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_solver_status import ExtractionSolverStatus


T = TypeVar("T", bound="ExtractionSolverDiagnostics")


@_attrs_define
class ExtractionSolverDiagnostics:
    """
    Attributes:
        classification (ExtractionSolverStatus | Unset):
        joint_ie (ExtractionSolverStatus | Unset):
        records (ExtractionSolverStatus | Unset):
    """

    classification: ExtractionSolverStatus | Unset = UNSET
    joint_ie: ExtractionSolverStatus | Unset = UNSET
    records: ExtractionSolverStatus | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        classification: dict[str, Any] | Unset = UNSET
        if not isinstance(self.classification, Unset):
            classification = self.classification.to_dict()

        joint_ie: dict[str, Any] | Unset = UNSET
        if not isinstance(self.joint_ie, Unset):
            joint_ie = self.joint_ie.to_dict()

        records: dict[str, Any] | Unset = UNSET
        if not isinstance(self.records, Unset):
            records = self.records.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if classification is not UNSET:
            field_dict["classification"] = classification
        if joint_ie is not UNSET:
            field_dict["joint_ie"] = joint_ie
        if records is not UNSET:
            field_dict["records"] = records

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_solver_status import ExtractionSolverStatus

        d = dict(src_dict)
        _classification = d.pop("classification", UNSET)
        classification: ExtractionSolverStatus | Unset
        if isinstance(_classification, Unset):
            classification = UNSET
        else:
            classification = ExtractionSolverStatus.from_dict(_classification)

        _joint_ie = d.pop("joint_ie", UNSET)
        joint_ie: ExtractionSolverStatus | Unset
        if isinstance(_joint_ie, Unset):
            joint_ie = UNSET
        else:
            joint_ie = ExtractionSolverStatus.from_dict(_joint_ie)

        _records = d.pop("records", UNSET)
        records: ExtractionSolverStatus | Unset
        if isinstance(_records, Unset):
            records = UNSET
        else:
            records = ExtractionSolverStatus.from_dict(_records)

        extraction_solver_diagnostics = cls(
            classification=classification,
            joint_ie=joint_ie,
            records=records,
        )

        return extraction_solver_diagnostics
