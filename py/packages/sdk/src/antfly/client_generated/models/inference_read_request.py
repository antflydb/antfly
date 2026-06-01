from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_image_url import InferenceImageURL


T = TypeVar("T", bound="InferenceReadRequest")


@_attrs_define
class InferenceReadRequest:
    """
    Attributes:
        model (str): Name of reader model from models_dir/readers/ Example: microsoft/trocr-base-printed.
        images (list[InferenceImageURL]): Images to read text from. Supports:
            - Data URIs: `data:image/png;base64,...`
            - URLs (if content_security allows)
             Example: [{'url': 'data:image/png;base64,iVBORw0KGgo...'}].
        prompt (str | Unset): Optional task prompt for document understanding models.
            - TrOCR: Not used (pure OCR)
            - Donut CORD: "<s_cord-v2>" for receipt parsing
            - Donut DocVQA: "<s_docvqa><s_question>What is the total?</s_question><s_answer>"
            - Florence-2: "<OCR>" for OCR, "<CAPTION>" for captioning
            - Pix2Struct: "What type of document is this?"
            - Moondream: "Describe this image."
             Example: What type of document is this?.
        max_tokens (int | Unset): Maximum tokens to generate Default: 256. Example: 256.
    """

    model: str
    images: list[InferenceImageURL]
    prompt: str | Unset = UNSET
    max_tokens: int | Unset = 256
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        images = []
        for images_item_data in self.images:
            images_item = images_item_data.to_dict()
            images.append(images_item)

        prompt = self.prompt

        max_tokens = self.max_tokens

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "images": images,
            }
        )
        if prompt is not UNSET:
            field_dict["prompt"] = prompt
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_image_url import InferenceImageURL

        d = dict(src_dict)
        model = d.pop("model")

        images = []
        _images = d.pop("images")
        for images_item_data in _images:
            images_item = InferenceImageURL.from_dict(images_item_data)

            images.append(images_item)

        prompt = d.pop("prompt", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        inference_read_request = cls(
            model=model,
            images=images,
            prompt=prompt,
            max_tokens=max_tokens,
        )

        inference_read_request.additional_properties = d
        return inference_read_request

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
