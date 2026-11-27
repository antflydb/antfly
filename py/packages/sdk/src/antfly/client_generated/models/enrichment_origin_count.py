from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EnrichmentOriginCount")


@_attrs_define
class EnrichmentOriginCount:
    """
    Attributes:
        name (str):
        kind (str): `asset`, `chunk` or `embedding`.
        seeded (int): Outputs accepted from artifact seeds for their current input.
        generated (int): Outputs Antfly's producers generated.
        external (int): Externally maintained assets and authored vectors.
        pending (int): Outputs held waiting for a seed (`on_missing: pending`), or, on serverless tables, awaiting
            generation.
        failed (int | Unset): Serverless only. Outputs whose generation failed its bounded retries for the current
            input.
    """

    name: str
    kind: str
    seeded: int
    generated: int
    external: int
    pending: int
    failed: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind

        seeded = self.seeded

        generated = self.generated

        external = self.external

        pending = self.pending

        failed = self.failed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "kind": kind,
                "seeded": seeded,
                "generated": generated,
                "external": external,
                "pending": pending,
            }
        )
        if failed is not UNSET:
            field_dict["failed"] = failed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        kind = d.pop("kind")

        seeded = d.pop("seeded")

        generated = d.pop("generated")

        external = d.pop("external")

        pending = d.pop("pending")

        failed = d.pop("failed", UNSET)

        enrichment_origin_count = cls(
            name=name,
            kind=kind,
            seeded=seeded,
            generated=generated,
            external=external,
            pending=pending,
            failed=failed,
        )

        enrichment_origin_count.additional_properties = d
        return enrichment_origin_count

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
