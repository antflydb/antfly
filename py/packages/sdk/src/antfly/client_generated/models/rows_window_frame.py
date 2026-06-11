from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_window_frame_end import RowsWindowFrameEnd
from ..models.rows_window_frame_start import RowsWindowFrameStart
from ..models.rows_window_frame_unit import RowsWindowFrameUnit
from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsWindowFrame")


@_attrs_define
class RowsWindowFrame:
    """
    Attributes:
        unit (RowsWindowFrameUnit):
        start (RowsWindowFrameStart):
        end (RowsWindowFrameEnd):
        start_offset (int | Unset):
        end_offset (int | Unset):
    """

    unit: RowsWindowFrameUnit
    start: RowsWindowFrameStart
    end: RowsWindowFrameEnd
    start_offset: int | Unset = UNSET
    end_offset: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        unit = self.unit.value

        start = self.start.value

        end = self.end.value

        start_offset = self.start_offset

        end_offset = self.end_offset

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "unit": unit,
                "start": start,
                "end": end,
            }
        )
        if start_offset is not UNSET:
            field_dict["start_offset"] = start_offset
        if end_offset is not UNSET:
            field_dict["end_offset"] = end_offset

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        unit = RowsWindowFrameUnit(d.pop("unit"))

        start = RowsWindowFrameStart(d.pop("start"))

        end = RowsWindowFrameEnd(d.pop("end"))

        start_offset = d.pop("start_offset", UNSET)

        end_offset = d.pop("end_offset", UNSET)

        rows_window_frame = cls(
            unit=unit,
            start=start,
            end=end,
            start_offset=start_offset,
            end_offset=end_offset,
        )

        return rows_window_frame
