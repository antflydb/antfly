from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.get_document_artifact_manifest_response_413_code import GetDocumentArtifactManifestResponse413Code

if TYPE_CHECKING:
    from ..models.get_document_artifact_manifest_response_413_limits import GetDocumentArtifactManifestResponse413Limits


T = TypeVar("T", bound="GetDocumentArtifactManifestResponse413")


@_attrs_define
class GetDocumentArtifactManifestResponse413:
    """
    Attributes:
        code (GetDocumentArtifactManifestResponse413Code):
        retryable (bool):
        message (str):
        limits (GetDocumentArtifactManifestResponse413Limits):
    """

    code: GetDocumentArtifactManifestResponse413Code
    retryable: bool
    message: str
    limits: GetDocumentArtifactManifestResponse413Limits
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        code = self.code.value

        retryable = self.retryable

        message = self.message

        limits = self.limits.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "code": code,
                "retryable": retryable,
                "message": message,
                "limits": limits,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.get_document_artifact_manifest_response_413_limits import (
            GetDocumentArtifactManifestResponse413Limits,
        )

        d = dict(src_dict)
        code = GetDocumentArtifactManifestResponse413Code(d.pop("code"))

        retryable = d.pop("retryable")

        message = d.pop("message")

        limits = GetDocumentArtifactManifestResponse413Limits.from_dict(d.pop("limits"))

        get_document_artifact_manifest_response_413 = cls(
            code=code,
            retryable=retryable,
            message=message,
            limits=limits,
        )

        get_document_artifact_manifest_response_413.additional_properties = d
        return get_document_artifact_manifest_response_413

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
