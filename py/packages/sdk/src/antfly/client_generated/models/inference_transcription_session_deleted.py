from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_transcription_session_deleted_object import InferenceTranscriptionSessionDeletedObject

T = TypeVar("T", bound="InferenceTranscriptionSessionDeleted")


@_attrs_define
class InferenceTranscriptionSessionDeleted:
    """
    Attributes:
        object_ (InferenceTranscriptionSessionDeletedObject):
        id (str):
        deleted (bool):
    """

    object_: InferenceTranscriptionSessionDeletedObject
    id: str
    deleted: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        id = self.id

        deleted = self.deleted

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "id": id,
                "deleted": deleted,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        object_ = InferenceTranscriptionSessionDeletedObject(d.pop("object"))

        id = d.pop("id")

        deleted = d.pop("deleted")

        inference_transcription_session_deleted = cls(
            object_=object_,
            id=id,
            deleted=deleted,
        )

        inference_transcription_session_deleted.additional_properties = d
        return inference_transcription_session_deleted

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
