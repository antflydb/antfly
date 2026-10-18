from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.image_url_content_part import ImageURLContentPart
    from ..models.media_content_part import MediaContentPart
    from ..models.text_content_part import TextContentPart


T = TypeVar("T", bound="InferenceEmbeddingContentInput")


@_attrs_define
class InferenceEmbeddingContentInput:
    """One ordered input producing one combined embedding. Supported by EmbeddingGemma 2. Text, image, and audio parts may
    be combined in order. On Linux CUDA, a group containing one video/mp4 media part produces one clip embedding using
    NVDEC; mixed video groups are unsupported. Video requires progressive SDR H.264 MP4, at most 60 seconds and 256 MiB,
    subject to narrower request limits. Sampling uses 1 FPS capped uniformly to 32 frames, with at most 140 visual
    tokens per frame. The expanded input, including task prompts, BOS/EOS, and media tokens, must fit within 8192
    tokens. Overflow is rejected without truncation.

        Attributes:
            content (list[ImageURLContentPart | MediaContentPart | TextContentPart]):
            title (str | Unset): Document title, allowed only with RETRIEVAL_DOCUMENT and at least one text part.
    """

    content: list[ImageURLContentPart | MediaContentPart | TextContentPart]
    title: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.text_content_part import TextContentPart

        content = []
        for content_item_data in self.content:
            content_item: dict[str, Any]
            if isinstance(content_item_data, TextContentPart):
                content_item = content_item_data.to_dict()
            elif isinstance(content_item_data, ImageURLContentPart):
                content_item = content_item_data.to_dict()
            else:
                content_item = content_item_data.to_dict()

            content.append(content_item)

        title = self.title

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "content": content,
            }
        )
        if title is not UNSET:
            field_dict["title"] = title

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart

        d = dict(src_dict)
        content = []
        _content = d.pop("content")
        for content_item_data in _content:

            def _parse_content_item(data: object) -> ImageURLContentPart | MediaContentPart | TextContentPart:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_content_part_type_0 = TextContentPart.from_dict(data)

                    return componentsschemas_content_part_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_content_part_type_1 = ImageURLContentPart.from_dict(data)

                    return componentsschemas_content_part_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_content_part_type_2 = MediaContentPart.from_dict(data)

                return componentsschemas_content_part_type_2

            content_item = _parse_content_item(content_item_data)

            content.append(content_item)

        title = d.pop("title", UNSET)

        inference_embedding_content_input = cls(
            content=content,
            title=title,
        )

        return inference_embedding_content_input
