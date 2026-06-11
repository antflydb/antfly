from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentArtifactReprocessJobStartRequest")


@_attrs_define
class DocumentArtifactReprocessJobStartRequest:
    """Request to create a durable table artifact reprocess job.

    Attributes:
        from_key (str | Unset): Exclusive lower bound source document key. Default: ''.
        to_key (str | Unset): Inclusive upper bound source document key, or empty for the end of the table/range.
            Default: ''.
        limit (int | Unset): Maximum source rows to scan per shard-local repair pass. Zero uses the server default.
            Default: 100.
        advance (bool | Unset): When true, immediately runs the first bounded pass before returning the job state.
            Default: True.
    """

    from_key: str | Unset = ""
    to_key: str | Unset = ""
    limit: int | Unset = 100
    advance: bool | Unset = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from_key = self.from_key

        to_key = self.to_key

        limit = self.limit

        advance = self.advance

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
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
        from_key = d.pop("from_key", UNSET)

        to_key = d.pop("to_key", UNSET)

        limit = d.pop("limit", UNSET)

        advance = d.pop("advance", UNSET)

        document_artifact_reprocess_job_start_request = cls(
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
