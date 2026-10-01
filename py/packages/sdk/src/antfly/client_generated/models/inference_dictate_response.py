from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_dictate_response_object import InferenceDictateResponseObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_dictation_transcript import InferenceDictationTranscript
    from ..models.inference_generate_usage import InferenceGenerateUsage


T = TypeVar("T", bound="InferenceDictateResponse")


@_attrs_define
class InferenceDictateResponse:
    """
    Attributes:
        object_ (InferenceDictateResponseObject):
        id (str):
        created (int): Unix timestamp (seconds).
        model (str): Transcriber model used.
        transcript (InferenceDictationTranscript):
        text (str): Cleaned text, or the raw transcript when no cleanup ran.
        usage (InferenceGenerateUsage):
        cleanup_model (str | Unset): Generator model used for cleanup, when one ran.
    """

    object_: InferenceDictateResponseObject
    id: str
    created: int
    model: str
    transcript: InferenceDictationTranscript
    text: str
    usage: InferenceGenerateUsage
    cleanup_model: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        id = self.id

        created = self.created

        model = self.model

        transcript = self.transcript.to_dict()

        text = self.text

        usage = self.usage.to_dict()

        cleanup_model = self.cleanup_model

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "id": id,
                "created": created,
                "model": model,
                "transcript": transcript,
                "text": text,
                "usage": usage,
            }
        )
        if cleanup_model is not UNSET:
            field_dict["cleanup_model"] = cleanup_model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_dictation_transcript import InferenceDictationTranscript
        from ..models.inference_generate_usage import InferenceGenerateUsage

        d = dict(src_dict)
        object_ = InferenceDictateResponseObject(d.pop("object"))

        id = d.pop("id")

        created = d.pop("created")

        model = d.pop("model")

        transcript = InferenceDictationTranscript.from_dict(d.pop("transcript"))

        text = d.pop("text")

        usage = InferenceGenerateUsage.from_dict(d.pop("usage"))

        cleanup_model = d.pop("cleanup_model", UNSET)

        inference_dictate_response = cls(
            object_=object_,
            id=id,
            created=created,
            model=model,
            transcript=transcript,
            text=text,
            usage=usage,
            cleanup_model=cleanup_model,
        )

        inference_dictate_response.additional_properties = d
        return inference_dictate_response

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
