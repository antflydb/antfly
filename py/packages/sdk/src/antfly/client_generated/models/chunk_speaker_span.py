from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ChunkSpeakerSpan")


@_attrs_define
class ChunkSpeakerSpan:
    """
    Attributes:
        start_ns (int):
        end_ns (int):
        speaker_index (int | Unset): Source-scoped voice index in order of first appearance; absent for silence or
            ambiguous audio.
    """

    start_ns: int
    end_ns: int
    speaker_index: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        start_ns = self.start_ns

        end_ns = self.end_ns

        speaker_index = self.speaker_index

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "start_ns": start_ns,
                "end_ns": end_ns,
            }
        )
        if speaker_index is not UNSET:
            field_dict["speaker_index"] = speaker_index

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        start_ns = d.pop("start_ns")

        end_ns = d.pop("end_ns")

        speaker_index = d.pop("speaker_index", UNSET)

        chunk_speaker_span = cls(
            start_ns=start_ns,
            end_ns=end_ns,
            speaker_index=speaker_index,
        )

        chunk_speaker_span.additional_properties = d
        return chunk_speaker_span

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
