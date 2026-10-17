from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chunk_source_reference import ChunkSourceReference
    from ..models.media_chunk_policy import MediaChunkPolicy


T = TypeVar("T", bound="ChunkEmbeddingContext")


@_attrs_define
class ChunkEmbeddingContext:
    """
    Attributes:
        source_fingerprint (str):
        source_version (str):
        plan_id (str):
        model (str):
        model_identity (str):
        model_revision (str):
        task_type (str):
        policy (MediaChunkPolicy): Semantic chunk policy. Execution batch size and queue depth are deliberately
            separate.
        start_ordinal (int):
        source_reference (ChunkSourceReference | Unset):
        source_id (str | Unset):
        dimensions (int | Unset): Known vector width; may be absent before a planning-only or failed first item.
    """

    source_fingerprint: str
    source_version: str
    plan_id: str
    model: str
    model_identity: str
    model_revision: str
    task_type: str
    policy: MediaChunkPolicy
    start_ordinal: int
    source_reference: ChunkSourceReference | Unset = UNSET
    source_id: str | Unset = UNSET
    dimensions: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source_fingerprint = self.source_fingerprint

        source_version = self.source_version

        plan_id = self.plan_id

        model = self.model

        model_identity = self.model_identity

        model_revision = self.model_revision

        task_type = self.task_type

        policy = self.policy.to_dict()

        start_ordinal = self.start_ordinal

        source_reference: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source_reference, Unset):
            source_reference = self.source_reference.to_dict()

        source_id = self.source_id

        dimensions = self.dimensions

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "source_fingerprint": source_fingerprint,
                "source_version": source_version,
                "plan_id": plan_id,
                "model": model,
                "model_identity": model_identity,
                "model_revision": model_revision,
                "task_type": task_type,
                "policy": policy,
                "start_ordinal": start_ordinal,
            }
        )
        if source_reference is not UNSET:
            field_dict["source_reference"] = source_reference
        if source_id is not UNSET:
            field_dict["source_id"] = source_id
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunk_source_reference import ChunkSourceReference
        from ..models.media_chunk_policy import MediaChunkPolicy

        d = dict(src_dict)
        source_fingerprint = d.pop("source_fingerprint")

        source_version = d.pop("source_version")

        plan_id = d.pop("plan_id")

        model = d.pop("model")

        model_identity = d.pop("model_identity")

        model_revision = d.pop("model_revision")

        task_type = d.pop("task_type")

        policy = MediaChunkPolicy.from_dict(d.pop("policy"))

        start_ordinal = d.pop("start_ordinal")

        _source_reference = d.pop("source_reference", UNSET)
        source_reference: ChunkSourceReference | Unset
        if isinstance(_source_reference, Unset):
            source_reference = UNSET
        else:
            source_reference = ChunkSourceReference.from_dict(_source_reference)

        source_id = d.pop("source_id", UNSET)

        dimensions = d.pop("dimensions", UNSET)

        chunk_embedding_context = cls(
            source_fingerprint=source_fingerprint,
            source_version=source_version,
            plan_id=plan_id,
            model=model,
            model_identity=model_identity,
            model_revision=model_revision,
            task_type=task_type,
            policy=policy,
            start_ordinal=start_ordinal,
            source_reference=source_reference,
            source_id=source_id,
            dimensions=dimensions,
        )

        chunk_embedding_context.additional_properties = d
        return chunk_embedding_context

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
