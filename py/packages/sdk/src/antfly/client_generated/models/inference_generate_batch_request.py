from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_generate_batch_mode import InferenceGenerateBatchMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_generate_batch_request_item import InferenceGenerateBatchRequestItem


T = TypeVar("T", bound="InferenceGenerateBatchRequest")


@_attrs_define
class InferenceGenerateBatchRequest:
    """
    Attributes:
        requests (list[InferenceGenerateBatchRequestItem]):
        mode (InferenceGenerateBatchMode | Unset): Batch execution mode. Only synchronous batches are implemented.
    """

    requests: list[InferenceGenerateBatchRequestItem]
    mode: InferenceGenerateBatchMode | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        requests = []
        for requests_item_data in self.requests:
            requests_item = requests_item_data.to_dict()
            requests.append(requests_item)

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "requests": requests,
            }
        )
        if mode is not UNSET:
            field_dict["mode"] = mode

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_generate_batch_request_item import InferenceGenerateBatchRequestItem

        d = dict(src_dict)
        requests = []
        _requests = d.pop("requests")
        for requests_item_data in _requests:
            requests_item = InferenceGenerateBatchRequestItem.from_dict(requests_item_data)

            requests.append(requests_item)

        _mode = d.pop("mode", UNSET)
        mode: InferenceGenerateBatchMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = InferenceGenerateBatchMode(_mode)

        inference_generate_batch_request = cls(
            requests=requests,
            mode=mode,
        )

        inference_generate_batch_request.additional_properties = d
        return inference_generate_batch_request

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
