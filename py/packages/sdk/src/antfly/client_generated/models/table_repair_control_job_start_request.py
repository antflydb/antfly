from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.table_repair_control_job_start_request_control import TableRepairControlJobStartRequestControl
from ..types import UNSET, Unset

T = TypeVar("T", bound="TableRepairControlJobStartRequest")


@_attrs_define
class TableRepairControlJobStartRequest:
    """Starts a durable named-index control traversal. The server advances every bounded pass, including after restart. Use
    the repair job status and cancellation endpoints to inspect or stop the traversal.

        Attributes:
            index (str): Index to control across the table.
            control (TableRepairControlJobStartRequestControl): Durable named-index control applied in bounded server-owned
                passes across every table group.
            repair_id (str | Unset): Decimal repair attempt fence, preserved across every pass. A stale fence fails the
                control job.
            cursor (str | Unset): Opaque continuation cursor from a prior bounded control response.
            limit (int | Unset):  Default: 100.
            advance (bool | Unset): Attempt the first bounded pass immediately. Remaining passes always run server-side.
                Default: True.
    """

    index: str
    control: TableRepairControlJobStartRequestControl
    repair_id: str | Unset = UNSET
    cursor: str | Unset = UNSET
    limit: int | Unset = 100
    advance: bool | Unset = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        control = self.control.value

        repair_id = self.repair_id

        cursor = self.cursor

        limit = self.limit

        advance = self.advance

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index": index,
                "control": control,
            }
        )
        if repair_id is not UNSET:
            field_dict["repair_id"] = repair_id
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if limit is not UNSET:
            field_dict["limit"] = limit
        if advance is not UNSET:
            field_dict["advance"] = advance

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index = d.pop("index")

        control = TableRepairControlJobStartRequestControl(d.pop("control"))

        repair_id = d.pop("repair_id", UNSET)

        cursor = d.pop("cursor", UNSET)

        limit = d.pop("limit", UNSET)

        advance = d.pop("advance", UNSET)

        table_repair_control_job_start_request = cls(
            index=index,
            control=control,
            repair_id=repair_id,
            cursor=cursor,
            limit=limit,
            advance=advance,
        )

        table_repair_control_job_start_request.additional_properties = d
        return table_repair_control_job_start_request

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
