from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceRequestLimitsConfig")


@_attrs_define
class InferenceRequestLimitsConfig:
    """Deployment request ceilings only narrow model capabilities. Applied equally to direct calls, HTTP and discovery.
    Omitted values retain native limits.

        Attributes:
            max_items_per_request (int | Unset): Maximum items in one public or direct request. Independent callers may
                still be combined into a larger native execution microbatch.
            max_input_tokens_per_item (int | Unset): Maximum effective input tokens per item, including expanded image
                prompts.
            max_output_tokens_per_item (int | Unset): Maximum generated output tokens per item. Omitted request max_tokens
                resolves to a bounded effective default.
            max_decoded_pixels_per_batch (int | Unset): Maximum aggregate decoded image pixels in a request batch; also
                bounds execution microbatch pixels.
    """

    max_items_per_request: int | Unset = UNSET
    max_input_tokens_per_item: int | Unset = UNSET
    max_output_tokens_per_item: int | Unset = UNSET
    max_decoded_pixels_per_batch: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        max_items_per_request = self.max_items_per_request

        max_input_tokens_per_item = self.max_input_tokens_per_item

        max_output_tokens_per_item = self.max_output_tokens_per_item

        max_decoded_pixels_per_batch = self.max_decoded_pixels_per_batch

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_items_per_request is not UNSET:
            field_dict["max_items_per_request"] = max_items_per_request
        if max_input_tokens_per_item is not UNSET:
            field_dict["max_input_tokens_per_item"] = max_input_tokens_per_item
        if max_output_tokens_per_item is not UNSET:
            field_dict["max_output_tokens_per_item"] = max_output_tokens_per_item
        if max_decoded_pixels_per_batch is not UNSET:
            field_dict["max_decoded_pixels_per_batch"] = max_decoded_pixels_per_batch

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_items_per_request = d.pop("max_items_per_request", UNSET)

        max_input_tokens_per_item = d.pop("max_input_tokens_per_item", UNSET)

        max_output_tokens_per_item = d.pop("max_output_tokens_per_item", UNSET)

        max_decoded_pixels_per_batch = d.pop("max_decoded_pixels_per_batch", UNSET)

        inference_request_limits_config = cls(
            max_items_per_request=max_items_per_request,
            max_input_tokens_per_item=max_input_tokens_per_item,
            max_output_tokens_per_item=max_output_tokens_per_item,
            max_decoded_pixels_per_batch=max_decoded_pixels_per_batch,
        )

        return inference_request_limits_config
