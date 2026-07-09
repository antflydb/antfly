from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_embedding_item_error_stage import InferenceEmbeddingItemErrorStage

T = TypeVar("T", bound="InferenceEmbeddingItemError")


@_attrs_define
class InferenceEmbeddingItemError:
    """Per-input embedding failure for error_policy=per_item responses

    Attributes:
        index (int): Original input index that failed
        code (str): Stable machine-readable failure code
        message (str): Human-readable failure message
        stage (InferenceEmbeddingItemErrorStage): Pipeline stage that classified the failure
        retryable (bool): Whether retrying the same item may succeed
        status (int): HTTP-style status classification for this item
    """

    index: int
    code: str
    message: str
    stage: InferenceEmbeddingItemErrorStage
    retryable: bool
    status: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        code = self.code

        message = self.message

        stage = self.stage.value

        retryable = self.retryable

        status = self.status

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index": index,
                "code": code,
                "message": message,
                "stage": stage,
                "retryable": retryable,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index = d.pop("index")

        code = d.pop("code")

        message = d.pop("message")

        stage = InferenceEmbeddingItemErrorStage(d.pop("stage"))

        retryable = d.pop("retryable")

        status = d.pop("status")

        inference_embedding_item_error = cls(
            index=index,
            code=code,
            message=message,
            stage=stage,
            retryable=retryable,
            status=status,
        )

        inference_embedding_item_error.additional_properties = d
        return inference_embedding_item_error

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
