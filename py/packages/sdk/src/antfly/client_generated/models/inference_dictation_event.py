from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_dictation_event_type import InferenceDictationEventType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_dictation_transcript import InferenceDictationTranscript
    from ..models.inference_generate_usage import InferenceGenerateUsage


T = TypeVar("T", bound="InferenceDictationEvent")


@_attrs_define
class InferenceDictationEvent:
    """One Server-Sent Event of a streaming dictation. `dictation.transcript`
    carries `transcript`; `dictation.delta` carries `delta`;
    `dictation.completed` carries `text` and `usage`; `error` carries
    `error` and `message`. The stream ends with the literal `[DONE]`.

        Attributes:
            type_ (InferenceDictationEventType):
            id (str):
            model (str | Unset):
            cleanup_model (str | Unset):
            transcript (InferenceDictationTranscript | Unset):
            delta (str | Unset):
            text (str | Unset):
            usage (InferenceGenerateUsage | Unset):
            error (str | Unset):
            message (str | Unset):
    """

    type_: InferenceDictationEventType
    id: str
    model: str | Unset = UNSET
    cleanup_model: str | Unset = UNSET
    transcript: InferenceDictationTranscript | Unset = UNSET
    delta: str | Unset = UNSET
    text: str | Unset = UNSET
    usage: InferenceGenerateUsage | Unset = UNSET
    error: str | Unset = UNSET
    message: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        id = self.id

        model = self.model

        cleanup_model = self.cleanup_model

        transcript: dict[str, Any] | Unset = UNSET
        if not isinstance(self.transcript, Unset):
            transcript = self.transcript.to_dict()

        delta = self.delta

        text = self.text

        usage: dict[str, Any] | Unset = UNSET
        if not isinstance(self.usage, Unset):
            usage = self.usage.to_dict()

        error = self.error

        message = self.message

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "id": id,
            }
        )
        if model is not UNSET:
            field_dict["model"] = model
        if cleanup_model is not UNSET:
            field_dict["cleanup_model"] = cleanup_model
        if transcript is not UNSET:
            field_dict["transcript"] = transcript
        if delta is not UNSET:
            field_dict["delta"] = delta
        if text is not UNSET:
            field_dict["text"] = text
        if usage is not UNSET:
            field_dict["usage"] = usage
        if error is not UNSET:
            field_dict["error"] = error
        if message is not UNSET:
            field_dict["message"] = message

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_dictation_transcript import InferenceDictationTranscript
        from ..models.inference_generate_usage import InferenceGenerateUsage

        d = dict(src_dict)
        type_ = InferenceDictationEventType(d.pop("type"))

        id = d.pop("id")

        model = d.pop("model", UNSET)

        cleanup_model = d.pop("cleanup_model", UNSET)

        _transcript = d.pop("transcript", UNSET)
        transcript: InferenceDictationTranscript | Unset
        if isinstance(_transcript, Unset):
            transcript = UNSET
        else:
            transcript = InferenceDictationTranscript.from_dict(_transcript)

        delta = d.pop("delta", UNSET)

        text = d.pop("text", UNSET)

        _usage = d.pop("usage", UNSET)
        usage: InferenceGenerateUsage | Unset
        if isinstance(_usage, Unset):
            usage = UNSET
        else:
            usage = InferenceGenerateUsage.from_dict(_usage)

        error = d.pop("error", UNSET)

        message = d.pop("message", UNSET)

        inference_dictation_event = cls(
            type_=type_,
            id=id,
            model=model,
            cleanup_model=cleanup_model,
            transcript=transcript,
            delta=delta,
            text=text,
            usage=usage,
            error=error,
            message=message,
        )

        inference_dictation_event.additional_properties = d
        return inference_dictation_event

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
