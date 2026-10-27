from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_document_job_operation import InferenceDocumentJobOperation
from ..models.inference_document_job_state import InferenceDocumentJobState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_document_job_usage import InferenceDocumentJobUsage


T = TypeVar("T", bound="InferenceDocumentJob")


@_attrs_define
class InferenceDocumentJob:
    """
    Attributes:
        id (str):
        operation (InferenceDocumentJobOperation):
        state (InferenceDocumentJobState):
        attempt (int):
        sequence (int):
        expires_at_ms (int):
        usage (InferenceDocumentJobUsage):
        error (str | Unset):
    """

    id: str
    operation: InferenceDocumentJobOperation
    state: InferenceDocumentJobState
    attempt: int
    sequence: int
    expires_at_ms: int
    usage: InferenceDocumentJobUsage
    error: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        operation = self.operation.value

        state = self.state.value

        attempt = self.attempt

        sequence = self.sequence

        expires_at_ms = self.expires_at_ms

        usage = self.usage.to_dict()

        error = self.error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "operation": operation,
                "state": state,
                "attempt": attempt,
                "sequence": sequence,
                "expires_at_ms": expires_at_ms,
                "usage": usage,
            }
        )
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_document_job_usage import InferenceDocumentJobUsage

        d = dict(src_dict)
        id = d.pop("id")

        operation = InferenceDocumentJobOperation(d.pop("operation"))

        state = InferenceDocumentJobState(d.pop("state"))

        attempt = d.pop("attempt")

        sequence = d.pop("sequence")

        expires_at_ms = d.pop("expires_at_ms")

        usage = InferenceDocumentJobUsage.from_dict(d.pop("usage"))

        error = d.pop("error", UNSET)

        inference_document_job = cls(
            id=id,
            operation=operation,
            state=state,
            attempt=attempt,
            sequence=sequence,
            expires_at_ms=expires_at_ms,
            usage=usage,
            error=error,
        )

        inference_document_job.additional_properties = d
        return inference_document_job

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
