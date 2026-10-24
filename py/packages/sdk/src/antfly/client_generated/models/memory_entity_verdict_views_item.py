from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemoryEntityVerdictViewsItem")


@_attrs_define
class MemoryEntityVerdictViewsItem:
    """
    Attributes:
        id (str):
        fingerprint (str):
    """

    id: str
    fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        fingerprint = self.fingerprint

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "fingerprint": fingerprint,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        fingerprint = d.pop("fingerprint")

        memory_entity_verdict_views_item = cls(
            id=id,
            fingerprint=fingerprint,
        )

        return memory_entity_verdict_views_item
