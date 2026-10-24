from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemoryClaimPin")


@_attrs_define
class MemoryClaimPin:
    """
    Attributes:
        id (str):
        revision (int):
        fingerprint (str):
    """

    id: str
    revision: int
    fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        revision = self.revision

        fingerprint = self.fingerprint

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "revision": revision,
                "fingerprint": fingerprint,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        revision = d.pop("revision")

        fingerprint = d.pop("fingerprint")

        memory_claim_pin = cls(
            id=id,
            revision=revision,
            fingerprint=fingerprint,
        )

        return memory_claim_pin
