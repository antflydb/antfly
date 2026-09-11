from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_long_document_metadata_classification_aggregation import (
    ExtractionLongDocumentMetadataClassificationAggregation,
)
from ..models.extraction_long_document_metadata_duplicate_score import ExtractionLongDocumentMetadataDuplicateScore
from ..models.extraction_long_document_metadata_natural_record_identity import (
    ExtractionLongDocumentMetadataNaturalRecordIdentity,
)
from ..models.extraction_long_document_metadata_other_record_identity import (
    ExtractionLongDocumentMetadataOtherRecordIdentity,
)
from ..models.extraction_long_document_metadata_solver_optimality_scope import (
    ExtractionLongDocumentMetadataSolverOptimalityScope,
)
from ..models.extraction_long_document_metadata_version import ExtractionLongDocumentMetadataVersion
from ..models.extraction_long_document_metadata_window_policy import ExtractionLongDocumentMetadataWindowPolicy

T = TypeVar("T", bound="ExtractionLongDocumentMetadata")


@_attrs_define
class ExtractionLongDocumentMetadata:
    """
    Attributes:
        version (ExtractionLongDocumentMetadataVersion):
        window_count (int):
        window_policy (ExtractionLongDocumentMetadataWindowPolicy):
        classification_aggregation (ExtractionLongDocumentMetadataClassificationAggregation):
        duplicate_score (ExtractionLongDocumentMetadataDuplicateScore):
        natural_record_identity (ExtractionLongDocumentMetadataNaturalRecordIdentity):
        other_record_identity (ExtractionLongDocumentMetadataOtherRecordIdentity):
        solver_optimality_scope (ExtractionLongDocumentMetadataSolverOptimalityScope):
    """

    version: ExtractionLongDocumentMetadataVersion
    window_count: int
    window_policy: ExtractionLongDocumentMetadataWindowPolicy
    classification_aggregation: ExtractionLongDocumentMetadataClassificationAggregation
    duplicate_score: ExtractionLongDocumentMetadataDuplicateScore
    natural_record_identity: ExtractionLongDocumentMetadataNaturalRecordIdentity
    other_record_identity: ExtractionLongDocumentMetadataOtherRecordIdentity
    solver_optimality_scope: ExtractionLongDocumentMetadataSolverOptimalityScope

    def to_dict(self) -> dict[str, Any]:
        version = self.version.value

        window_count = self.window_count

        window_policy = self.window_policy.value

        classification_aggregation = self.classification_aggregation.value

        duplicate_score = self.duplicate_score.value

        natural_record_identity = self.natural_record_identity.value

        other_record_identity = self.other_record_identity.value

        solver_optimality_scope = self.solver_optimality_scope.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "version": version,
                "window_count": window_count,
                "window_policy": window_policy,
                "classification_aggregation": classification_aggregation,
                "duplicate_score": duplicate_score,
                "natural_record_identity": natural_record_identity,
                "other_record_identity": other_record_identity,
                "solver_optimality_scope": solver_optimality_scope,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        version = ExtractionLongDocumentMetadataVersion(d.pop("version"))

        window_count = d.pop("window_count")

        window_policy = ExtractionLongDocumentMetadataWindowPolicy(d.pop("window_policy"))

        classification_aggregation = ExtractionLongDocumentMetadataClassificationAggregation(
            d.pop("classification_aggregation")
        )

        duplicate_score = ExtractionLongDocumentMetadataDuplicateScore(d.pop("duplicate_score"))

        natural_record_identity = ExtractionLongDocumentMetadataNaturalRecordIdentity(d.pop("natural_record_identity"))

        other_record_identity = ExtractionLongDocumentMetadataOtherRecordIdentity(d.pop("other_record_identity"))

        solver_optimality_scope = ExtractionLongDocumentMetadataSolverOptimalityScope(d.pop("solver_optimality_scope"))

        extraction_long_document_metadata = cls(
            version=version,
            window_count=window_count,
            window_policy=window_policy,
            classification_aggregation=classification_aggregation,
            duplicate_score=duplicate_score,
            natural_record_identity=natural_record_identity,
            other_record_identity=other_record_identity,
            solver_optimality_scope=solver_optimality_scope,
        )

        return extraction_long_document_metadata
