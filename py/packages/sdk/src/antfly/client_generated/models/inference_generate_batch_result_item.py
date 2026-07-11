from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_generate_batch_error import InferenceGenerateBatchError
    from ..models.inference_generate_response import InferenceGenerateResponse


T = TypeVar("T", bound="InferenceGenerateBatchResultItem")


@_attrs_define
class InferenceGenerateBatchResultItem:
    """
    Attributes:
        custom_id (str):
        index (int): Zero-based request index from the submitted batch.
        response (InferenceGenerateResponse | Unset): OpenAI-compatible chat completion response
        error (InferenceGenerateBatchError | Unset):
    """

    custom_id: str
    index: int
    response: InferenceGenerateResponse | Unset = UNSET
    error: InferenceGenerateBatchError | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        custom_id = self.custom_id

        index = self.index

        response: dict[str, Any] | Unset = UNSET
        if not isinstance(self.response, Unset):
            response = self.response.to_dict()

        error: dict[str, Any] | Unset = UNSET
        if not isinstance(self.error, Unset):
            error = self.error.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "custom_id": custom_id,
                "index": index,
            }
        )
        if response is not UNSET:
            field_dict["response"] = response
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_generate_batch_error import InferenceGenerateBatchError
        from ..models.inference_generate_response import InferenceGenerateResponse

        d = dict(src_dict)
        custom_id = d.pop("custom_id")

        index = d.pop("index")

        _response = d.pop("response", UNSET)
        response: InferenceGenerateResponse | Unset
        if isinstance(_response, Unset):
            response = UNSET
        else:
            response = InferenceGenerateResponse.from_dict(_response)

        _error = d.pop("error", UNSET)
        error: InferenceGenerateBatchError | Unset
        if isinstance(_error, Unset):
            error = UNSET
        else:
            error = InferenceGenerateBatchError.from_dict(_error)

        inference_generate_batch_result_item = cls(
            custom_id=custom_id,
            index=index,
            response=response,
            error=error,
        )

        inference_generate_batch_result_item.additional_properties = d
        return inference_generate_batch_result_item

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
