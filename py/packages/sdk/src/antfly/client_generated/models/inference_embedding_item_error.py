from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_embedding_item_error_stage import InferenceEmbeddingItemErrorStage
from ..types import UNSET, Unset

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
        retry_after_ms (int | None | Unset): Minimum retry delay in milliseconds for a retryable transient failure
    """

    index: int
    code: str
    message: str
    stage: InferenceEmbeddingItemErrorStage
    retryable: bool
    status: int
    retry_after_ms: int | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        code = self.code

        message = self.message

        stage = self.stage.value

        retryable = self.retryable

        status = self.status

        retry_after_ms: int | None | Unset
        if isinstance(self.retry_after_ms, Unset):
            retry_after_ms = UNSET
        else:
            retry_after_ms = self.retry_after_ms

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
        if retry_after_ms is not UNSET:
            field_dict["retry_after_ms"] = retry_after_ms

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

        def _parse_retry_after_ms(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        retry_after_ms = _parse_retry_after_ms(d.pop("retry_after_ms", UNSET))

        inference_embedding_item_error = cls(
            index=index,
            code=code,
            message=message,
            stage=stage,
            retryable=retryable,
            status=status,
            retry_after_ms=retry_after_ms,
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
