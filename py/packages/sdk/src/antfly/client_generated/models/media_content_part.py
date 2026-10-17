from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.media_content_part_type import MediaContentPartType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.video_sampling_policy import VideoSamplingPolicy


T = TypeVar("T", bound="MediaContentPart")


@_attrs_define
class MediaContentPart:
    """Inline binary media content (audio, image, etc.).

    Attributes:
        type_ (MediaContentPartType):
        data (str): Base64-encoded binary data.
        mime_type (str): MIME type (audio/wav, image/gif, image/png, etc.).
        sampling (VideoSamplingPolicy | Unset): Explicit EmbeddingGemma 2 video-group sampling. Timestamp windows are
            half-open and relative to the first presentation picture.
    """

    type_: MediaContentPartType
    data: str
    mime_type: str
    sampling: VideoSamplingPolicy | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        data = self.data

        mime_type = self.mime_type

        sampling: dict[str, Any] | Unset = UNSET
        if not isinstance(self.sampling, Unset):
            sampling = self.sampling.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "data": data,
                "mime_type": mime_type,
            }
        )
        if sampling is not UNSET:
            field_dict["sampling"] = sampling

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.video_sampling_policy import VideoSamplingPolicy

        d = dict(src_dict)
        type_ = MediaContentPartType(d.pop("type"))

        data = d.pop("data")

        mime_type = d.pop("mime_type")

        _sampling = d.pop("sampling", UNSET)
        sampling: VideoSamplingPolicy | Unset
        if isinstance(_sampling, Unset):
            sampling = UNSET
        else:
            sampling = VideoSamplingPolicy.from_dict(_sampling)

        media_content_part = cls(
            type_=type_,
            data=data,
            mime_type=mime_type,
            sampling=sampling,
        )

        media_content_part.additional_properties = d
        return media_content_part

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
