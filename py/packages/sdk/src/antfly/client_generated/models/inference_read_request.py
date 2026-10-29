from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_source import DocumentSource
    from ..models.inference_image_url import InferenceImageURL


T = TypeVar("T", bound="InferenceReadRequest")


@_attrs_define
class InferenceReadRequest:
    """Read exactly one of images, documents, or audio. Audio reads use an audio-capable generator such as Gemma 4 and
    transcribe the audio track of supported video containers.

        Attributes:
            model (str): Reader model from models_dir/readers/, or an audio-capable generator from models_dir/generators/
                for audio inputs. Example: microsoft/trocr-base-printed.
            audio (str | Unset): Base64-encoded audio or a data URI, mutually exclusive with images and documents. Supports
                audio tracks in MP4/MOV and WebM containers. Uses an audio-capable generator from models_dir/generators/ such as
                Gemma 4. Returns transcript text without word timestamps or speaker labels; video frames are not captioned. Use
                a registered document reader with an audio binding for planning, fallback, verification, artifact retention, and
                durable jobs.
            retain_artifact (bool | Unset): Request an opaque, process-local reading handle for extraction reuse. Retention
                is best effort; inspect document.artifact_id and artifact_ttl_ms. Default: False.
            documents (list[DocumentSource] | Unset): Complete documents, mutually exclusive with images. Each source is
                independent; selected pages are read in original source order.
            images (list[InferenceImageURL] | Unset): Images to read text from. Supports:
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
    audio: str | Unset = UNSET
    retain_artifact: bool | Unset = False
    documents: list[DocumentSource] | Unset = UNSET
    images: list[InferenceImageURL] | Unset = UNSET
    prompt: str | Unset = UNSET
    max_tokens: int | Unset = 256
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        audio = self.audio

        retain_artifact = self.retain_artifact

        documents: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.documents, Unset):
            documents = []
            for documents_item_data in self.documents:
                documents_item = documents_item_data.to_dict()
                documents.append(documents_item)

        images: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.images, Unset):
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
            }
        )
        if audio is not UNSET:
            field_dict["audio"] = audio
        if retain_artifact is not UNSET:
            field_dict["retain_artifact"] = retain_artifact
        if documents is not UNSET:
            field_dict["documents"] = documents
        if images is not UNSET:
            field_dict["images"] = images
        if prompt is not UNSET:
            field_dict["prompt"] = prompt
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_source import DocumentSource
        from ..models.inference_image_url import InferenceImageURL

        d = dict(src_dict)
        model = d.pop("model")

        audio = d.pop("audio", UNSET)

        retain_artifact = d.pop("retain_artifact", UNSET)

        _documents = d.pop("documents", UNSET)
        documents: list[DocumentSource] | Unset = UNSET
        if _documents is not UNSET:
            documents = []
            for documents_item_data in _documents:
                documents_item = DocumentSource.from_dict(documents_item_data)

                documents.append(documents_item)

        _images = d.pop("images", UNSET)
        images: list[InferenceImageURL] | Unset = UNSET
        if _images is not UNSET:
            images = []
            for images_item_data in _images:
                images_item = InferenceImageURL.from_dict(images_item_data)

                images.append(images_item)

        prompt = d.pop("prompt", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        inference_read_request = cls(
            model=model,
            audio=audio,
            retain_artifact=retain_artifact,
            documents=documents,
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
