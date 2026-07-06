from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.repair_target import RepairTarget

if TYPE_CHECKING:
    from ..models.artifact_repair_run_result import ArtifactRepairRunResult


T = TypeVar("T", bound="ArtifactRepairRunResponse")


@_attrs_define
class ArtifactRepairRunResponse:
    """Response for a bounded table repair pass.

    Attributes:
        table (str): Table whose repair queue was processed.
        target (RepairTarget): Repair subsystem to inspect or run.
        limit (int): Effective repair limit.
        result (ArtifactRepairRunResult): Result of one bounded artifact repair pass.
    """

    table: str
    target: RepairTarget
    limit: int
    result: ArtifactRepairRunResult
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        target = self.target.value

        limit = self.limit

        result = self.result.to_dict()

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

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.artifact_repair_run_result import ArtifactRepairRunResult

        d = dict(src_dict)
        table = d.pop("table")

        target = RepairTarget(d.pop("target"))

        limit = d.pop("limit")

        result = ArtifactRepairRunResult.from_dict(d.pop("result"))

        artifact_repair_run_response = cls(
            table=table,
            target=target,
            limit=limit,
            result=result,
        )

        artifact_repair_run_response.additional_properties = d
        return artifact_repair_run_response

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
