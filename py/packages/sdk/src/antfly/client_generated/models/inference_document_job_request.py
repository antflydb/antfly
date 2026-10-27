from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_document_job_request_request import InferenceDocumentJobRequestRequest


T = TypeVar("T", bound="InferenceDocumentJobRequest")


@_attrs_define
class InferenceDocumentJobRequest:
    """Durable document execution on the unified Antfly API. Requires configured durable job storage and local inference
    orchestration. The request uses the ordinary read or extract schema. Credentials must use server connections,
    environment variables, or secret references.

        Attributes:
            request (InferenceDocumentJobRequestRequest):
            retention_seconds (int | Unset):  Default: 86400.
    """

    request: InferenceDocumentJobRequestRequest
    retention_seconds: int | Unset = 86400

    def to_dict(self) -> dict[str, Any]:
        request = self.request.to_dict()

        retention_seconds = self.retention_seconds

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "request": request,
            }
        )
        if retention_seconds is not UNSET:
            field_dict["retention_seconds"] = retention_seconds

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_document_job_request_request import InferenceDocumentJobRequestRequest

        d = dict(src_dict)
        request = InferenceDocumentJobRequestRequest.from_dict(d.pop("request"))

        retention_seconds = d.pop("retention_seconds", UNSET)

        inference_document_job_request = cls(
            request=request,
            retention_seconds=retention_seconds,
        )

        return inference_document_job_request
