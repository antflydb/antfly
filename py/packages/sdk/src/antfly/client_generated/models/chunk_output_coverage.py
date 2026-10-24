from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ChunkOutputCoverage")


@_attrs_define
class ChunkOutputCoverage:
    """
    Attributes:
        name (str):
        required (bool):
        complete (int):
        failed (int):
        skipped (int | Unset):
    """

    name: str
    required: bool
    complete: int
    failed: int
    skipped: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        required = self.required

        complete = self.complete

        failed = self.failed

        skipped = self.skipped

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "required": required,
                "complete": complete,
                "failed": failed,
            }
        )
        if skipped is not UNSET:
            field_dict["skipped"] = skipped

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        required = d.pop("required")

        complete = d.pop("complete")

        failed = d.pop("failed")

        skipped = d.pop("skipped", UNSET)

        chunk_output_coverage = cls(
            name=name,
            required=required,
            complete=complete,
            failed=failed,
            skipped=skipped,
        )

        chunk_output_coverage.additional_properties = d
        return chunk_output_coverage

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
