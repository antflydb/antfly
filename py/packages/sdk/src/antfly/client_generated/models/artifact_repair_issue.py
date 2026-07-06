from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.artifact_repair_kind import ArtifactRepairKind
from ..models.artifact_repair_reason import ArtifactRepairReason
from ..types import UNSET, Unset

T = TypeVar("T", bound="ArtifactRepairIssue")


@_attrs_define
class ArtifactRepairIssue:
    """Durable repair debt for a derived artifact. This is an operator-facing record and includes exact source and artifact
    identifiers.

        Attributes:
            artifact_kind (ArtifactRepairKind): Kind of stored artifact tracked by the repair queue.
            index_name (str): Index whose replay or derived state observed the artifact problem.
            doc_key (str): Source or derived document key whose artifact is missing or unreadable.
            artifact_name (str): Derived artifact name that must be reprocessed or made readable.
            repairable (bool): Whether this artifact kind currently has an automated repair reprocessor.
            sequence (int): Derived replay sequence that observed the issue.
            reason (ArtifactRepairReason): Reason an artifact was added to the repair queue.
            attempts (int): Number of repair attempts made for this issue.
            first_seen_ns (int): Monotonic timestamp when this issue was first recorded.
            last_seen_ns (int): Monotonic timestamp when this issue was last observed or attempted.
            parent_doc_key (str | Unset): Parent source document key for chunk-derived artifacts.
            unit_id (str | Unset): Unit identifier for unit-scoped artifacts, when applicable.
            source_artifact_name (str | Unset): Source artifact stream used to produce this artifact, when applicable.
            artifact_key (str | Unset): Hex-encoded internal artifact storage key, when known.
            chunk_id (int | None | Unset): Chunk ordinal for chunk-derived artifacts.
            unsupported_reason (str | Unset): Stable reason code when repairable is false.
            last_error (str | Unset): Last stable repair error code, when a repair attempt failed.
    """

    artifact_kind: ArtifactRepairKind
    index_name: str
    doc_key: str
    artifact_name: str
    repairable: bool
    sequence: int
    reason: ArtifactRepairReason
    attempts: int
    first_seen_ns: int
    last_seen_ns: int
    parent_doc_key: str | Unset = UNSET
    unit_id: str | Unset = UNSET
    source_artifact_name: str | Unset = UNSET
    artifact_key: str | Unset = UNSET
    chunk_id: int | None | Unset = UNSET
    unsupported_reason: str | Unset = UNSET
    last_error: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        artifact_kind = self.artifact_kind.value

        index_name = self.index_name

        doc_key = self.doc_key

        artifact_name = self.artifact_name

        repairable = self.repairable

        sequence = self.sequence

        reason = self.reason.value

        attempts = self.attempts

        first_seen_ns = self.first_seen_ns

        last_seen_ns = self.last_seen_ns

        parent_doc_key = self.parent_doc_key

        unit_id = self.unit_id

        source_artifact_name = self.source_artifact_name

        artifact_key = self.artifact_key

        chunk_id: int | None | Unset
        if isinstance(self.chunk_id, Unset):
            chunk_id = UNSET
        else:
            chunk_id = self.chunk_id

        unsupported_reason = self.unsupported_reason

        last_error = self.last_error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "artifact_kind": artifact_kind,
                "index_name": index_name,
                "doc_key": doc_key,
                "artifact_name": artifact_name,
                "repairable": repairable,
                "sequence": sequence,
                "reason": reason,
                "attempts": attempts,
                "first_seen_ns": first_seen_ns,
                "last_seen_ns": last_seen_ns,
            }
        )
        if parent_doc_key is not UNSET:
            field_dict["parent_doc_key"] = parent_doc_key
        if unit_id is not UNSET:
            field_dict["unit_id"] = unit_id
        if source_artifact_name is not UNSET:
            field_dict["source_artifact_name"] = source_artifact_name
        if artifact_key is not UNSET:
            field_dict["artifact_key"] = artifact_key
        if chunk_id is not UNSET:
            field_dict["chunk_id"] = chunk_id
        if unsupported_reason is not UNSET:
            field_dict["unsupported_reason"] = unsupported_reason
        if last_error is not UNSET:
            field_dict["last_error"] = last_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        artifact_kind = ArtifactRepairKind(d.pop("artifact_kind"))

        index_name = d.pop("index_name")

        doc_key = d.pop("doc_key")

        artifact_name = d.pop("artifact_name")

        repairable = d.pop("repairable")

        sequence = d.pop("sequence")

        reason = ArtifactRepairReason(d.pop("reason"))

        attempts = d.pop("attempts")

        first_seen_ns = d.pop("first_seen_ns")

        last_seen_ns = d.pop("last_seen_ns")

        parent_doc_key = d.pop("parent_doc_key", UNSET)

        unit_id = d.pop("unit_id", UNSET)

        source_artifact_name = d.pop("source_artifact_name", UNSET)

        artifact_key = d.pop("artifact_key", UNSET)

        def _parse_chunk_id(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        chunk_id = _parse_chunk_id(d.pop("chunk_id", UNSET))

        unsupported_reason = d.pop("unsupported_reason", UNSET)

        last_error = d.pop("last_error", UNSET)

        artifact_repair_issue = cls(
            artifact_kind=artifact_kind,
            index_name=index_name,
            doc_key=doc_key,
            artifact_name=artifact_name,
            repairable=repairable,
            sequence=sequence,
            reason=reason,
            attempts=attempts,
            first_seen_ns=first_seen_ns,
            last_seen_ns=last_seen_ns,
            parent_doc_key=parent_doc_key,
            unit_id=unit_id,
            source_artifact_name=source_artifact_name,
            artifact_key=artifact_key,
            chunk_id=chunk_id,
            unsupported_reason=unsupported_reason,
            last_error=last_error,
        )

        artifact_repair_issue.additional_properties = d
        return artifact_repair_issue

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
