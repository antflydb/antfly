from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_artifact_reprocess_response_reprocess import DocumentArtifactReprocessResponseReprocess
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentArtifactReprocessResponse")


@_attrs_define
class DocumentArtifactReprocessResponse:
    """
    Attributes:
        reprocess (DocumentArtifactReprocessResponseReprocess): Indicates that reprocessing was accepted.
        accepted_sequence (int | Unset): Committed replay sequence for a deferred native extraction request. Scope to
            the accepting shard and the artifact identified by this route.
    """

    reprocess: DocumentArtifactReprocessResponseReprocess
    accepted_sequence: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        reprocess = self.reprocess.value

        accepted_sequence = self.accepted_sequence

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "reprocess": reprocess,
            }
        )
        if accepted_sequence is not UNSET:
            field_dict["accepted_sequence"] = accepted_sequence

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        reprocess = DocumentArtifactReprocessResponseReprocess(d.pop("reprocess"))

        accepted_sequence = d.pop("accepted_sequence", UNSET)

        document_artifact_reprocess_response = cls(
            reprocess=reprocess,
            accepted_sequence=accepted_sequence,
        )

        document_artifact_reprocess_response.additional_properties = d
        return document_artifact_reprocess_response

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
