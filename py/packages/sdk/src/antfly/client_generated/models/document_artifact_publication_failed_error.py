from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.document_artifact_publication_failed_error_code import DocumentArtifactPublicationFailedErrorCode
from ..models.document_artifact_publication_failed_error_error import DocumentArtifactPublicationFailedErrorError

T = TypeVar("T", bound="DocumentArtifactPublicationFailedError")


@_attrs_define
class DocumentArtifactPublicationFailedError:
    """The requested source artifact failed publication and requires inspection and reprocessing.

    Attributes:
        code (DocumentArtifactPublicationFailedErrorCode):
        error (DocumentArtifactPublicationFailedErrorError):
        message (str):
        retryable (bool):
    """

    code: DocumentArtifactPublicationFailedErrorCode
    error: DocumentArtifactPublicationFailedErrorError
    message: str
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        code = self.code.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "code": code,
                "error": error,
                "message": message,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = DocumentArtifactPublicationFailedErrorCode(d.pop("code"))

        error = DocumentArtifactPublicationFailedErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        document_artifact_publication_failed_error = cls(
            code=code,
            error=error,
            message=message,
            retryable=retryable,
        )

        return document_artifact_publication_failed_error
