from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunk_embedding_response_object import ChunkEmbeddingResponseObject
from ..models.chunk_embedding_response_status import ChunkEmbeddingResponseStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chunk_coordinates import ChunkCoordinates
    from ..models.chunk_embedding_error import ChunkEmbeddingError
    from ..models.chunk_embedding_object import ChunkEmbeddingObject
    from ..models.chunk_output_coverage import ChunkOutputCoverage
    from ..models.chunk_source_reference import ChunkSourceReference
    from ..models.media_chunk_policy import MediaChunkPolicy


T = TypeVar("T", bound="ChunkEmbeddingResponse")


@_attrs_define
class ChunkEmbeddingResponse:
    """
    Attributes:
        object_ (ChunkEmbeddingResponseObject):
        data (list[ChunkEmbeddingObject]):
        status (ChunkEmbeddingResponseStatus): Completion of this response's suffix starting at start_ordinal; callers
            must retain earlier acknowledged coverage.
        source_fingerprint (str):
        source_version (str):
        plan_id (str):
        model (str):
        model_identity (str):
        model_revision (str): Model generation revision used by inference capability leases.
        task_type (str):
        dimensions (int):
        policy (MediaChunkPolicy): Semantic chunk policy. Execution batch size and queue depth are deliberately
            separate.
        start_ordinal (int):
        covered_through_ordinal (int): Exclusive end of acknowledged output ordinals; failed chunk coordinates are
            reported in gaps.
        gaps (list[ChunkCoordinates]):
        output_coverage (list[ChunkOutputCoverage] | Unset):
        degraded (bool | Unset): Optional outputs failed without preventing required-output coverage.
        source_complete (bool | Unset): True when source enumeration reached EOF, independently of per-output gaps.
            Resume enumeration at covered_through_ordinal; repair gaps separately.
        source_reference (ChunkSourceReference | Unset):
        source_id (str | Unset):
        error (ChunkEmbeddingError | Unset):
        next_continuation (str | Unset):
    """

    object_: ChunkEmbeddingResponseObject
    data: list[ChunkEmbeddingObject]
    status: ChunkEmbeddingResponseStatus
    source_fingerprint: str
    source_version: str
    plan_id: str
    model: str
    model_identity: str
    model_revision: str
    task_type: str
    dimensions: int
    policy: MediaChunkPolicy
    start_ordinal: int
    covered_through_ordinal: int
    gaps: list[ChunkCoordinates]
    output_coverage: list[ChunkOutputCoverage] | Unset = UNSET
    degraded: bool | Unset = UNSET
    source_complete: bool | Unset = UNSET
    source_reference: ChunkSourceReference | Unset = UNSET
    source_id: str | Unset = UNSET
    error: ChunkEmbeddingError | Unset = UNSET
    next_continuation: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        data = []
        for data_item_data in self.data:
            data_item = data_item_data.to_dict()
            data.append(data_item)

        status = self.status.value

        source_fingerprint = self.source_fingerprint

        source_version = self.source_version

        plan_id = self.plan_id

        model = self.model

        model_identity = self.model_identity

        model_revision = self.model_revision

        task_type = self.task_type

        dimensions = self.dimensions

        policy = self.policy.to_dict()

        start_ordinal = self.start_ordinal

        covered_through_ordinal = self.covered_through_ordinal

        gaps = []
        for gaps_item_data in self.gaps:
            gaps_item = gaps_item_data.to_dict()
            gaps.append(gaps_item)

        output_coverage: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.output_coverage, Unset):
            output_coverage = []
            for output_coverage_item_data in self.output_coverage:
                output_coverage_item = output_coverage_item_data.to_dict()
                output_coverage.append(output_coverage_item)

        degraded = self.degraded

        source_complete = self.source_complete

        source_reference: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source_reference, Unset):
            source_reference = self.source_reference.to_dict()

        source_id = self.source_id

        error: dict[str, Any] | Unset = UNSET
        if not isinstance(self.error, Unset):
            error = self.error.to_dict()

        next_continuation = self.next_continuation

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "data": data,
                "status": status,
                "source_fingerprint": source_fingerprint,
                "source_version": source_version,
                "plan_id": plan_id,
                "model": model,
                "model_identity": model_identity,
                "model_revision": model_revision,
                "task_type": task_type,
                "dimensions": dimensions,
                "policy": policy,
                "start_ordinal": start_ordinal,
                "covered_through_ordinal": covered_through_ordinal,
                "gaps": gaps,
            }
        )
        if output_coverage is not UNSET:
            field_dict["output_coverage"] = output_coverage
        if degraded is not UNSET:
            field_dict["degraded"] = degraded
        if source_complete is not UNSET:
            field_dict["source_complete"] = source_complete
        if source_reference is not UNSET:
            field_dict["source_reference"] = source_reference
        if source_id is not UNSET:
            field_dict["source_id"] = source_id
        if error is not UNSET:
            field_dict["error"] = error
        if next_continuation is not UNSET:
            field_dict["next_continuation"] = next_continuation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunk_coordinates import ChunkCoordinates
        from ..models.chunk_embedding_error import ChunkEmbeddingError
        from ..models.chunk_embedding_object import ChunkEmbeddingObject
        from ..models.chunk_output_coverage import ChunkOutputCoverage
        from ..models.chunk_source_reference import ChunkSourceReference
        from ..models.media_chunk_policy import MediaChunkPolicy

        d = dict(src_dict)
        object_ = ChunkEmbeddingResponseObject(d.pop("object"))

        data = []
        _data = d.pop("data")
        for data_item_data in _data:
            data_item = ChunkEmbeddingObject.from_dict(data_item_data)

            data.append(data_item)

        status = ChunkEmbeddingResponseStatus(d.pop("status"))

        source_fingerprint = d.pop("source_fingerprint")

        source_version = d.pop("source_version")

        plan_id = d.pop("plan_id")

        model = d.pop("model")

        model_identity = d.pop("model_identity")

        model_revision = d.pop("model_revision")

        task_type = d.pop("task_type")

        dimensions = d.pop("dimensions")

        policy = MediaChunkPolicy.from_dict(d.pop("policy"))

        start_ordinal = d.pop("start_ordinal")

        covered_through_ordinal = d.pop("covered_through_ordinal")

        gaps = []
        _gaps = d.pop("gaps")
        for gaps_item_data in _gaps:
            gaps_item = ChunkCoordinates.from_dict(gaps_item_data)

            gaps.append(gaps_item)

        _output_coverage = d.pop("output_coverage", UNSET)
        output_coverage: list[ChunkOutputCoverage] | Unset = UNSET
        if _output_coverage is not UNSET:
            output_coverage = []
            for output_coverage_item_data in _output_coverage:
                output_coverage_item = ChunkOutputCoverage.from_dict(output_coverage_item_data)

                output_coverage.append(output_coverage_item)

        degraded = d.pop("degraded", UNSET)

        source_complete = d.pop("source_complete", UNSET)

        _source_reference = d.pop("source_reference", UNSET)
        source_reference: ChunkSourceReference | Unset
        if isinstance(_source_reference, Unset):
            source_reference = UNSET
        else:
            source_reference = ChunkSourceReference.from_dict(_source_reference)

        source_id = d.pop("source_id", UNSET)

        _error = d.pop("error", UNSET)
        error: ChunkEmbeddingError | Unset
        if isinstance(_error, Unset):
            error = UNSET
        else:
            error = ChunkEmbeddingError.from_dict(_error)

        next_continuation = d.pop("next_continuation", UNSET)

        chunk_embedding_response = cls(
            object_=object_,
            data=data,
            status=status,
            source_fingerprint=source_fingerprint,
            source_version=source_version,
            plan_id=plan_id,
            model=model,
            model_identity=model_identity,
            model_revision=model_revision,
            task_type=task_type,
            dimensions=dimensions,
            policy=policy,
            start_ordinal=start_ordinal,
            covered_through_ordinal=covered_through_ordinal,
            gaps=gaps,
            output_coverage=output_coverage,
            degraded=degraded,
            source_complete=source_complete,
            source_reference=source_reference,
            source_id=source_id,
            error=error,
            next_continuation=next_continuation,
        )

        chunk_embedding_response.additional_properties = d
        return chunk_embedding_response

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
