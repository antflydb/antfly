from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceBatchExecutionReport")


@_attrs_define
class InferenceBatchExecutionReport:
    """Observed executor behavior, not a capability prediction.

    Attributes:
        requested_items (int):
        native_batches (int):
        native_items (int):
        serial_items (int):
        rejected_items (int): Items rejected before model execution by validation, resolution, or admission.
        fallback_items (int):
        fallback_reason (None | str | Unset):
    """

    requested_items: int
    native_batches: int
    native_items: int
    serial_items: int
    rejected_items: int
    fallback_items: int
    fallback_reason: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        requested_items = self.requested_items

        native_batches = self.native_batches

        native_items = self.native_items

        serial_items = self.serial_items

        rejected_items = self.rejected_items

        fallback_items = self.fallback_items

        fallback_reason: None | str | Unset
        if isinstance(self.fallback_reason, Unset):
            fallback_reason = UNSET
        else:
            fallback_reason = self.fallback_reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "requested_items": requested_items,
                "native_batches": native_batches,
                "native_items": native_items,
                "serial_items": serial_items,
                "rejected_items": rejected_items,
                "fallback_items": fallback_items,
            }
        )
        if fallback_reason is not UNSET:
            field_dict["fallback_reason"] = fallback_reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        requested_items = d.pop("requested_items")

        native_batches = d.pop("native_batches")

        native_items = d.pop("native_items")

        serial_items = d.pop("serial_items")

        rejected_items = d.pop("rejected_items")

        fallback_items = d.pop("fallback_items")

        def _parse_fallback_reason(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        fallback_reason = _parse_fallback_reason(d.pop("fallback_reason", UNSET))

        inference_batch_execution_report = cls(
            requested_items=requested_items,
            native_batches=native_batches,
            native_items=native_items,
            serial_items=serial_items,
            rejected_items=rejected_items,
            fallback_items=fallback_items,
            fallback_reason=fallback_reason,
        )

        inference_batch_execution_report.additional_properties = d
        return inference_batch_execution_report

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
