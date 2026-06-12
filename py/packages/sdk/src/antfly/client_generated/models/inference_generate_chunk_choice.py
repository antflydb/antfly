from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_finish_reason import InferenceFinishReason
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_generate_delta import InferenceGenerateDelta


T = TypeVar("T", bound="InferenceGenerateChunkChoice")


@_attrs_define
class InferenceGenerateChunkChoice:
    """
    Attributes:
        index (int):
        delta (InferenceGenerateDelta): Delta content for streaming
        finish_reason (InferenceFinishReason | Unset): Reason why generation stopped
    """

    index: int
    delta: InferenceGenerateDelta
    finish_reason: InferenceFinishReason | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        delta = self.delta.to_dict()

        finish_reason: str | Unset = UNSET
        if not isinstance(self.finish_reason, Unset):
            finish_reason = self.finish_reason.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index": index,
                "delta": delta,
            }
        )
        if finish_reason is not UNSET:
            field_dict["finish_reason"] = finish_reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_generate_delta import InferenceGenerateDelta

        d = dict(src_dict)
        index = d.pop("index")

        delta = InferenceGenerateDelta.from_dict(d.pop("delta"))

        _finish_reason = d.pop("finish_reason", UNSET)
        finish_reason: InferenceFinishReason | Unset
        if isinstance(_finish_reason, Unset):
            finish_reason = UNSET
        else:
            finish_reason = InferenceFinishReason(_finish_reason)

        inference_generate_chunk_choice = cls(
            index=index,
            delta=delta,
            finish_reason=finish_reason,
        )

        inference_generate_chunk_choice.additional_properties = d
        return inference_generate_chunk_choice

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
