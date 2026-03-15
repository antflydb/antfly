from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.join_strategy import JoinStrategy
from ..types import UNSET, Unset

T = TypeVar("T", bound="JoinProfile")


@_attrs_define
class JoinProfile:
    """Join execution statistics.

    Attributes:
        strategy_used (Union[Unset, JoinStrategy]): Strategy for executing the join:
            - `broadcast`: Broadcast small table to all shards of large table.
              Best for dimension tables < 10MB. O(small_table) memory per shard.
            - `index_lookup`: Use batch key lookups via indexes.
              Best for selective joins with indexed join keys. Low memory overhead.
            - `shuffle`: Hash-partition both tables by join key.
              Best for large-large table joins. Requires data movement.
        left_rows_scanned (Union[Unset, int]): Number of rows scanned from the left table.
        right_rows_scanned (Union[Unset, int]): Number of rows scanned from the right table.
        rows_matched (Union[Unset, int]): Number of rows that matched the join condition.
        rows_unmatched_left (Union[Unset, int]): Number of left rows without a match (for left/full joins).
        rows_unmatched_right (Union[Unset, int]): Number of right rows without a match (for right/full joins).
        duration_ms (Union[Unset, int]): Time spent executing the join in milliseconds.
    """

    strategy_used: Union[Unset, JoinStrategy] = UNSET
    left_rows_scanned: Union[Unset, int] = UNSET
    right_rows_scanned: Union[Unset, int] = UNSET
    rows_matched: Union[Unset, int] = UNSET
    rows_unmatched_left: Union[Unset, int] = UNSET
    rows_unmatched_right: Union[Unset, int] = UNSET
    duration_ms: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        strategy_used: Union[Unset, str] = UNSET
        if not isinstance(self.strategy_used, Unset):
            strategy_used = self.strategy_used.value

        left_rows_scanned = self.left_rows_scanned

        right_rows_scanned = self.right_rows_scanned

        rows_matched = self.rows_matched

        rows_unmatched_left = self.rows_unmatched_left

        rows_unmatched_right = self.rows_unmatched_right

        duration_ms = self.duration_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if strategy_used is not UNSET:
            field_dict["strategy_used"] = strategy_used
        if left_rows_scanned is not UNSET:
            field_dict["left_rows_scanned"] = left_rows_scanned
        if right_rows_scanned is not UNSET:
            field_dict["right_rows_scanned"] = right_rows_scanned
        if rows_matched is not UNSET:
            field_dict["rows_matched"] = rows_matched
        if rows_unmatched_left is not UNSET:
            field_dict["rows_unmatched_left"] = rows_unmatched_left
        if rows_unmatched_right is not UNSET:
            field_dict["rows_unmatched_right"] = rows_unmatched_right
        if duration_ms is not UNSET:
            field_dict["duration_ms"] = duration_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _strategy_used = d.pop("strategy_used", UNSET)
        strategy_used: Union[Unset, JoinStrategy]
        if isinstance(_strategy_used, Unset):
            strategy_used = UNSET
        else:
            strategy_used = JoinStrategy(_strategy_used)

        left_rows_scanned = d.pop("left_rows_scanned", UNSET)

        right_rows_scanned = d.pop("right_rows_scanned", UNSET)

        rows_matched = d.pop("rows_matched", UNSET)

        rows_unmatched_left = d.pop("rows_unmatched_left", UNSET)

        rows_unmatched_right = d.pop("rows_unmatched_right", UNSET)

        duration_ms = d.pop("duration_ms", UNSET)

        join_profile = cls(
            strategy_used=strategy_used,
            left_rows_scanned=left_rows_scanned,
            right_rows_scanned=right_rows_scanned,
            rows_matched=rows_matched,
            rows_unmatched_left=rows_unmatched_left,
            rows_unmatched_right=rows_unmatched_right,
            duration_ms=duration_ms,
        )

        join_profile.additional_properties = d
        return join_profile

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
