from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.inference_generate_request import InferenceGenerateRequest


T = TypeVar("T", bound="InferenceGenerateBatchRequestItem")


@_attrs_define
class InferenceGenerateBatchRequestItem:
    """
    Attributes:
        custom_id (str): Caller-supplied identifier echoed in the result item.
        body (InferenceGenerateRequest):
    """

    custom_id: str
    body: InferenceGenerateRequest
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        custom_id = self.custom_id

        body = self.body.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "custom_id": custom_id,
                "body": body,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_generate_request import InferenceGenerateRequest

        d = dict(src_dict)
        custom_id = d.pop("custom_id")

        body = InferenceGenerateRequest.from_dict(d.pop("body"))

        inference_generate_batch_request_item = cls(
            custom_id=custom_id,
            body=body,
        )

        inference_generate_batch_request_item.additional_properties = d
        return inference_generate_batch_request_item

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
