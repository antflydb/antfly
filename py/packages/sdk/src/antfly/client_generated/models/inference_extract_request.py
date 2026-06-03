from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_extract_request_schema import InferenceExtractRequestSchema
    from ..models.inference_image_url import InferenceImageURL


T = TypeVar("T", bound="InferenceExtractRequest")


@_attrs_define
class InferenceExtractRequest:
    """Exactly one of `texts` or `images` must be provided.
    When using `images`, the server selects a compatible reader internally
    and processes the request as: read document text -> run structured extraction.

        Attributes:
            model (str): Name of extractor model with 'extraction' capability Example: fastino/gliner2-base-v1.
            schema (InferenceExtractRequestSchema): Extraction schema mapping structure names to field definitions.
                Each field is defined as "field_name::type" where type is "str" or "list".
                Optional choice fields: "field_name::[opt1|opt2]::str".
                If no type is specified, defaults to "str".
                 Example: {'person': ['name::str', 'age::str', 'company::str']}.
            texts (list[str] | Unset): Texts to extract structured data from Example: ['John Smith is 30 years old and works
                at Google.'].
            images (list[InferenceImageURL] | Unset): Optional images to extract structured data from.
                When provided, the server first reads document text with a compatible reader
                and then runs schema extraction on the read text.
            prompt (str | Unset): Optional read-stage prompt used only when `images` are provided.
                Passed through to the reader before schema extraction.
                 Example: <OCR>.
            max_tokens (int | Unset): Maximum tokens for the read stage when `images` are provided.
                Ignored for text-only extraction requests.
                 Default: 256.
            threshold (float | Unset): Score threshold for span extraction (0.0-1.0) Default: 0.3.
            flat_ner (bool | Unset): If true, don't allow nested/overlapping entities Default: True.
            include_confidence (bool | Unset): If true, include confidence scores in output Default: False.
            include_spans (bool | Unset): If true, include character offset spans in output Default: False.
    """

    model: str
    schema: InferenceExtractRequestSchema
    texts: list[str] | Unset = UNSET
    images: list[InferenceImageURL] | Unset = UNSET
    prompt: str | Unset = UNSET
    max_tokens: int | Unset = 256
    threshold: float | Unset = 0.3
    flat_ner: bool | Unset = True
    include_confidence: bool | Unset = False
    include_spans: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        schema = self.schema.to_dict()

        texts: list[str] | Unset = UNSET
        if not isinstance(self.texts, Unset):
            texts = self.texts

        images: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.images, Unset):
            images = []
            for images_item_data in self.images:
                images_item = images_item_data.to_dict()
                images.append(images_item)

        prompt = self.prompt

        max_tokens = self.max_tokens

        threshold = self.threshold

        flat_ner = self.flat_ner

        include_confidence = self.include_confidence

        include_spans = self.include_spans

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "schema": schema,
            }
        )
        if texts is not UNSET:
            field_dict["texts"] = texts
        if images is not UNSET:
            field_dict["images"] = images
        if prompt is not UNSET:
            field_dict["prompt"] = prompt
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if flat_ner is not UNSET:
            field_dict["flat_ner"] = flat_ner
        if include_confidence is not UNSET:
            field_dict["include_confidence"] = include_confidence
        if include_spans is not UNSET:
            field_dict["include_spans"] = include_spans

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_extract_request_schema import InferenceExtractRequestSchema
        from ..models.inference_image_url import InferenceImageURL

        d = dict(src_dict)
        model = d.pop("model")

        schema = InferenceExtractRequestSchema.from_dict(d.pop("schema"))

        texts = cast(list[str], d.pop("texts", UNSET))

        _images = d.pop("images", UNSET)
        images: list[InferenceImageURL] | Unset = UNSET
        if _images is not UNSET:
            images = []
            for images_item_data in _images:
                images_item = InferenceImageURL.from_dict(images_item_data)

                images.append(images_item)

        prompt = d.pop("prompt", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        threshold = d.pop("threshold", UNSET)

        flat_ner = d.pop("flat_ner", UNSET)

        include_confidence = d.pop("include_confidence", UNSET)

        include_spans = d.pop("include_spans", UNSET)

        inference_extract_request = cls(
            model=model,
            schema=schema,
            texts=texts,
            images=images,
            prompt=prompt,
            max_tokens=max_tokens,
            threshold=threshold,
            flat_ner=flat_ner,
            include_confidence=include_confidence,
            include_spans=include_spans,
        )

        inference_extract_request.additional_properties = d
        return inference_extract_request

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
