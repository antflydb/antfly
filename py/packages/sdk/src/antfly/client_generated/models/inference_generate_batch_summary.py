from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="InferenceGenerateBatchSummary")


@_attrs_define
class InferenceGenerateBatchSummary:
    """
    Attributes:
        total (int):
        succeeded (int):
        failed (int):
    """

    total: int
    succeeded: int
    failed: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        total = self.total

        succeeded = self.succeeded

        failed = self.failed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "total": total,
                "succeeded": succeeded,
                "failed": failed,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        total = d.pop("total")

        succeeded = d.pop("succeeded")

        failed = d.pop("failed")

        inference_generate_batch_summary = cls(
            total=total,
            succeeded=succeeded,
            failed=failed,
        )

        inference_generate_batch_summary.additional_properties = d
        return inference_generate_batch_summary

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
