from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_transcription_session_object import InferenceTranscriptionSessionObject
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceTranscriptionSession")


@_attrs_define
class InferenceTranscriptionSession:
    """
    Attributes:
        object_ (InferenceTranscriptionSessionObject):
        id (str):
        model (str):
        created (int): Unix timestamp (seconds).
        expires_at (int): Unix timestamp (seconds) after which the session is reclaimed unless audio is appended.
        buffered_ms (int): Audio held for the open segment.
        total_ms (int): Audio appended over the session lifetime.
        finals (int):
        partials (int):
        language (str | Unset):
    """

    object_: InferenceTranscriptionSessionObject
    id: str
    model: str
    created: int
    expires_at: int
    buffered_ms: int
    total_ms: int
    finals: int
    partials: int
    language: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        id = self.id

        model = self.model

        created = self.created

        expires_at = self.expires_at

        buffered_ms = self.buffered_ms

        total_ms = self.total_ms

        finals = self.finals

        partials = self.partials

        language = self.language

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "id": id,
                "model": model,
                "created": created,
                "expires_at": expires_at,
                "buffered_ms": buffered_ms,
                "total_ms": total_ms,
                "finals": finals,
                "partials": partials,
            }
        )
        if language is not UNSET:
            field_dict["language"] = language

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        object_ = InferenceTranscriptionSessionObject(d.pop("object"))

        id = d.pop("id")

        model = d.pop("model")

        created = d.pop("created")

        expires_at = d.pop("expires_at")

        buffered_ms = d.pop("buffered_ms")

        total_ms = d.pop("total_ms")

        finals = d.pop("finals")

        partials = d.pop("partials")

        language = d.pop("language", UNSET)

        inference_transcription_session = cls(
            object_=object_,
            id=id,
            model=model,
            created=created,
            expires_at=expires_at,
            buffered_ms=buffered_ms,
            total_ms=total_ms,
            finals=finals,
            partials=partials,
            language=language,
        )

        inference_transcription_session.additional_properties = d
        return inference_transcription_session

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
