from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsDocKeyRange")


@_attrs_define
class RowsDocKeyRange:
    """Internal physical range selector used after durable range ownership
    routing. Public REST/SDK endpoints reject this field; it is not stable
    public row identity. At least one of `start` or `end` must be present,
    and a bounded range must have `start < end`.

        Attributes:
            start (str | Unset): Inclusive physical row-key lower bound.
            end (str | Unset): Exclusive physical row-key upper bound.
    """

    start: str | Unset = UNSET
    end: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        start = self.start

        end = self.end

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if start is not UNSET:
            field_dict["start"] = start
        if end is not UNSET:
            field_dict["end"] = end

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        start = d.pop("start", UNSET)

        end = d.pop("end", UNSET)

        rows_doc_key_range = cls(
            start=start,
            end=end,
        )

        return rows_doc_key_range
