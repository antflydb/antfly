from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.repair_target import RepairTarget
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.repair_remaining_failure import RepairRemainingFailure
    from ..models.table_repair_run_result import TableRepairRunResult


T = TypeVar("T", bound="TableRepairRunResponse")


@_attrs_define
class TableRepairRunResponse:
    """Response for a bounded table repair pass.

    Attributes:
        table (str): Table whose repair queue was processed.
        target (RepairTarget): Repair subsystem to inspect or run.
        limit (int): Effective repair limit.
        result (TableRepairRunResult): Result of one bounded table repair pass.
        remaining_failures (list[RepairRemainingFailure] | Unset): Documents that still fail after this pass, grouped by
            error code with the next step. Omitted for control passes.
        remaining_failures_truncated (bool | Unset): Whether more repair issues exist than the summary scanned.
    """

    table: str
    target: RepairTarget
    limit: int
    result: TableRepairRunResult
    remaining_failures: list[RepairRemainingFailure] | Unset = UNSET
    remaining_failures_truncated: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        target = self.target.value

        limit = self.limit

        result = self.result.to_dict()

        remaining_failures: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.remaining_failures, Unset):
            remaining_failures = []
            for remaining_failures_item_data in self.remaining_failures:
                remaining_failures_item = remaining_failures_item_data.to_dict()
                remaining_failures.append(remaining_failures_item)

        remaining_failures_truncated = self.remaining_failures_truncated

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
                "target": target,
                "limit": limit,
                "result": result,
            }
        )
        if remaining_failures is not UNSET:
            field_dict["remaining_failures"] = remaining_failures
        if remaining_failures_truncated is not UNSET:
            field_dict["remaining_failures_truncated"] = remaining_failures_truncated

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.repair_remaining_failure import RepairRemainingFailure
        from ..models.table_repair_run_result import TableRepairRunResult

        d = dict(src_dict)
        table = d.pop("table")

        target = RepairTarget(d.pop("target"))

        limit = d.pop("limit")

        result = TableRepairRunResult.from_dict(d.pop("result"))

        _remaining_failures = d.pop("remaining_failures", UNSET)
        remaining_failures: list[RepairRemainingFailure] | Unset = UNSET
        if _remaining_failures is not UNSET:
            remaining_failures = []
            for remaining_failures_item_data in _remaining_failures:
                remaining_failures_item = RepairRemainingFailure.from_dict(remaining_failures_item_data)

                remaining_failures.append(remaining_failures_item)

        remaining_failures_truncated = d.pop("remaining_failures_truncated", UNSET)

        table_repair_run_response = cls(
            table=table,
            target=target,
            limit=limit,
            result=result,
            remaining_failures=remaining_failures,
            remaining_failures_truncated=remaining_failures_truncated,
        )

        table_repair_run_response.additional_properties = d
        return table_repair_run_response

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
