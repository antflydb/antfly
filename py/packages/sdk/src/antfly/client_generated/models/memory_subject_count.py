from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemorySubjectCount")


@_attrs_define
class MemorySubjectCount:
    """
    Attributes:
        subject (str):
        memories (int):
    """

    subject: str
    memories: int

    def to_dict(self) -> dict[str, Any]:
        subject = self.subject

        memories = self.memories

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "subject": subject,
                "memories": memories,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        subject = d.pop("subject")

        memories = d.pop("memories")

        memory_subject_count = cls(
            subject=subject,
            memories=memories,
        )

        return memory_subject_count
