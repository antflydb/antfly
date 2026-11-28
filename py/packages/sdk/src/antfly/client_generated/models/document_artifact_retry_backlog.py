from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_artifact_retry_backlog_admission_blocked_by import DocumentArtifactRetryBacklogAdmissionBlockedBy
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentArtifactRetryBacklog")


@_attrs_define
class DocumentArtifactRetryBacklog:
    """Shard-wide retry debt sampled from the replay worker.

    Attributes:
        active_tickets (int):
        sleeping_tickets (int):
        oldest_unresolved_sequence (int | Unset):
        admission_blocked_by (DocumentArtifactRetryBacklogAdmissionBlockedBy | Unset): Admission constraint preventing
            replay from admitting fresh work, when blocked.
    """

    active_tickets: int
    sleeping_tickets: int
    oldest_unresolved_sequence: int | Unset = UNSET
    admission_blocked_by: DocumentArtifactRetryBacklogAdmissionBlockedBy | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        active_tickets = self.active_tickets

        sleeping_tickets = self.sleeping_tickets

        oldest_unresolved_sequence = self.oldest_unresolved_sequence

        admission_blocked_by: str | Unset = UNSET
        if not isinstance(self.admission_blocked_by, Unset):
            admission_blocked_by = self.admission_blocked_by.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "active_tickets": active_tickets,
                "sleeping_tickets": sleeping_tickets,
            }
        )
        if oldest_unresolved_sequence is not UNSET:
            field_dict["oldest_unresolved_sequence"] = oldest_unresolved_sequence
        if admission_blocked_by is not UNSET:
            field_dict["admission_blocked_by"] = admission_blocked_by

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        active_tickets = d.pop("active_tickets")

        sleeping_tickets = d.pop("sleeping_tickets")

        oldest_unresolved_sequence = d.pop("oldest_unresolved_sequence", UNSET)

        _admission_blocked_by = d.pop("admission_blocked_by", UNSET)
        admission_blocked_by: DocumentArtifactRetryBacklogAdmissionBlockedBy | Unset
        if isinstance(_admission_blocked_by, Unset):
            admission_blocked_by = UNSET
        else:
            admission_blocked_by = DocumentArtifactRetryBacklogAdmissionBlockedBy(_admission_blocked_by)

        document_artifact_retry_backlog = cls(
            active_tickets=active_tickets,
            sleeping_tickets=sleeping_tickets,
            oldest_unresolved_sequence=oldest_unresolved_sequence,
            admission_blocked_by=admission_blocked_by,
        )

        document_artifact_retry_backlog.additional_properties = d
        return document_artifact_retry_backlog

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
