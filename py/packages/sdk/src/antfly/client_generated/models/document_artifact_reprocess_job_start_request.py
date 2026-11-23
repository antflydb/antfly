from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_artifact_reprocess_job_start_request_selection import (
    DocumentArtifactReprocessJobStartRequestSelection,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentArtifactReprocessJobStartRequest")


@_attrs_define
class DocumentArtifactReprocessJobStartRequest:
    """Request to create a durable table artifact reprocess job.

    Attributes:
        selection (DocumentArtifactReprocessJobStartRequestSelection | Unset): all explicitly regenerates selected
            source records, including accepted artifacts. incomplete selects only missing or incomplete document_extraction
            artifacts, including converged manifests with failed OCR pages. Complete accepted results are skipped. Other
            producer kinds reject incomplete selection. Selection and cursors are retained by durable jobs. Acceptance does
            not certify inference completion. Default: DocumentArtifactReprocessJobStartRequestSelection.ALL.
        from_key (str | Unset): Exclusive lower bound source document key. Default: ''.
        to_key (str | Unset): Inclusive upper bound source document key, or empty for the end of the table/range.
            Default: ''.
        limit (int | Unset): Maximum source rows to scan per shard-local repair pass. Zero uses the server default.
            Default: 100.
        advance (bool | Unset): When true, immediately runs the first bounded pass before returning the job state.
            Default: True.
    """

    selection: DocumentArtifactReprocessJobStartRequestSelection | Unset = (
        DocumentArtifactReprocessJobStartRequestSelection.ALL
    )
    from_key: str | Unset = ""
    to_key: str | Unset = ""
    limit: int | Unset = 100
    advance: bool | Unset = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        selection: str | Unset = UNSET
        if not isinstance(self.selection, Unset):
            selection = self.selection.value

        from_key = self.from_key

        to_key = self.to_key

        limit = self.limit

        advance = self.advance

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if selection is not UNSET:
            field_dict["selection"] = selection
        if from_key is not UNSET:
            field_dict["from_key"] = from_key
        if to_key is not UNSET:
            field_dict["to_key"] = to_key
        if limit is not UNSET:
            field_dict["limit"] = limit
        if advance is not UNSET:
            field_dict["advance"] = advance

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _selection = d.pop("selection", UNSET)
        selection: DocumentArtifactReprocessJobStartRequestSelection | Unset
        if isinstance(_selection, Unset):
            selection = UNSET
        else:
            selection = DocumentArtifactReprocessJobStartRequestSelection(_selection)

        from_key = d.pop("from_key", UNSET)

        to_key = d.pop("to_key", UNSET)

        limit = d.pop("limit", UNSET)

        advance = d.pop("advance", UNSET)

        document_artifact_reprocess_job_start_request = cls(
            selection=selection,
            from_key=from_key,
            to_key=to_key,
            limit=limit,
            advance=advance,
        )

        document_artifact_reprocess_job_start_request.additional_properties = d
        return document_artifact_reprocess_job_start_request

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
