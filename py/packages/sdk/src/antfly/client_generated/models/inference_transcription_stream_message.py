from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_transcription_stream_message_type import InferenceTranscriptionStreamMessageType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_transcription_event import InferenceTranscriptionEvent


T = TypeVar("T", bound="InferenceTranscriptionStreamMessage")


@_attrs_define
class InferenceTranscriptionStreamMessage:
    """One Server-Sent Event on a session event stream. `session.open` starts
    the stream, `transcription.event` carries `event`, `ping` keeps the
    connection alive, `session.closed` ends it, and `error` carries
    `error` and `message`. The stream ends with the literal `[DONE]`.

        Attributes:
            type_ (InferenceTranscriptionStreamMessageType):
            session_id (str):
            event (InferenceTranscriptionEvent | Unset):
            buffered_ms (int | Unset):
            total_ms (int | Unset):
            error (str | Unset):
            message (str | Unset):
    """

    type_: InferenceTranscriptionStreamMessageType
    session_id: str
    event: InferenceTranscriptionEvent | Unset = UNSET
    buffered_ms: int | Unset = UNSET
    total_ms: int | Unset = UNSET
    error: str | Unset = UNSET
    message: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        session_id = self.session_id

        event: dict[str, Any] | Unset = UNSET
        if not isinstance(self.event, Unset):
            event = self.event.to_dict()

        buffered_ms = self.buffered_ms

        total_ms = self.total_ms

        error = self.error

        message = self.message

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "session_id": session_id,
            }
        )
        if event is not UNSET:
            field_dict["event"] = event
        if buffered_ms is not UNSET:
            field_dict["buffered_ms"] = buffered_ms
        if total_ms is not UNSET:
            field_dict["total_ms"] = total_ms
        if error is not UNSET:
            field_dict["error"] = error
        if message is not UNSET:
            field_dict["message"] = message

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_transcription_event import InferenceTranscriptionEvent

        d = dict(src_dict)
        type_ = InferenceTranscriptionStreamMessageType(d.pop("type"))

        session_id = d.pop("session_id")

        _event = d.pop("event", UNSET)
        event: InferenceTranscriptionEvent | Unset
        if isinstance(_event, Unset):
            event = UNSET
        else:
            event = InferenceTranscriptionEvent.from_dict(_event)

        buffered_ms = d.pop("buffered_ms", UNSET)

        total_ms = d.pop("total_ms", UNSET)

        error = d.pop("error", UNSET)

        message = d.pop("message", UNSET)

        inference_transcription_stream_message = cls(
            type_=type_,
            session_id=session_id,
            event=event,
            buffered_ms=buffered_ms,
            total_ms=total_ms,
            error=error,
            message=message,
        )

        inference_transcription_stream_message.additional_properties = d
        return inference_transcription_stream_message

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
