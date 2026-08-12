from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.join_strategy import JoinStrategy
from ..models.join_type import JoinType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.join_condition import JoinCondition
    from ..models.join_filters import JoinFilters


T = TypeVar("T", bound="JoinClause")


@_attrs_define
class JoinClause:
    """Configuration for joining data from another table.
    Supports inner, left, and right joins with automatic strategy selection.

        Attributes:
            right_table (str): Name of the table to join with. Example: customers.
            on (JoinCondition): Condition for matching rows between tables.
            join_type (JoinType | Unset): Type of join to perform:
                - `inner`: Only return rows with matches in both tables
                - `left`: Return all rows from left table, NULL for non-matching right rows
                - `right`: Return all rows from right table, NULL for non-matching left rows
            right_filters (JoinFilters | Unset): Filters to apply to a table before joining.
            right_fields (list[str] | Unset): Fields to include from the right table in the result.
                If not specified, all fields from the right table are included.
                Fields are prefixed with the right table name in the result.
                 Example: ['name', 'email', 'tier'].
            strategy_hint (JoinStrategy | Unset): Strategy for executing the join:
                - `broadcast`: Broadcast small table to all shards of large table.
                  Best for dimension tables < 10MB. O(small_table) memory per shard.
                - `index_lookup`: Use batch key lookups via indexes.
                  Best for selective joins with indexed join keys. Low memory overhead.
                - `shuffle`: Hash-partition both tables by join key.
                  Best for large-large table joins. Requires data movement.
            nested_join (JoinClause | Unset): Configuration for joining data from another table.
                Supports inner, left, and right joins with automatic strategy selection.
    """

    right_table: str
    on: JoinCondition
    join_type: JoinType | Unset = UNSET
    right_filters: JoinFilters | Unset = UNSET
    right_fields: list[str] | Unset = UNSET
    strategy_hint: JoinStrategy | Unset = UNSET
    nested_join: JoinClause | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        right_table = self.right_table

        on = self.on.to_dict()

        join_type: str | Unset = UNSET
        if not isinstance(self.join_type, Unset):
            join_type = self.join_type.value

        right_filters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.right_filters, Unset):
            right_filters = self.right_filters.to_dict()

        right_fields: list[str] | Unset = UNSET
        if not isinstance(self.right_fields, Unset):
            right_fields = self.right_fields

        strategy_hint: str | Unset = UNSET
        if not isinstance(self.strategy_hint, Unset):
            strategy_hint = self.strategy_hint.value

        nested_join: dict[str, Any] | Unset = UNSET
        if not isinstance(self.nested_join, Unset):
            nested_join = self.nested_join.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "right_table": right_table,
                "on": on,
            }
        )
        if join_type is not UNSET:
            field_dict["join_type"] = join_type
        if right_filters is not UNSET:
            field_dict["right_filters"] = right_filters
        if right_fields is not UNSET:
            field_dict["right_fields"] = right_fields
        if strategy_hint is not UNSET:
            field_dict["strategy_hint"] = strategy_hint
        if nested_join is not UNSET:
            field_dict["nested_join"] = nested_join

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.join_condition import JoinCondition
        from ..models.join_filters import JoinFilters

        d = dict(src_dict)
        right_table = d.pop("right_table")

        on = JoinCondition.from_dict(d.pop("on"))

        _join_type = d.pop("join_type", UNSET)
        join_type: JoinType | Unset
        if isinstance(_join_type, Unset):
            join_type = UNSET
        else:
            join_type = JoinType(_join_type)

        _right_filters = d.pop("right_filters", UNSET)
        right_filters: JoinFilters | Unset
        if isinstance(_right_filters, Unset):
            right_filters = UNSET
        else:
            right_filters = JoinFilters.from_dict(_right_filters)

        right_fields = cast(list[str], d.pop("right_fields", UNSET))

        _strategy_hint = d.pop("strategy_hint", UNSET)
        strategy_hint: JoinStrategy | Unset
        if isinstance(_strategy_hint, Unset):
            strategy_hint = UNSET
        else:
            strategy_hint = JoinStrategy(_strategy_hint)

        _nested_join = d.pop("nested_join", UNSET)
        nested_join: JoinClause | Unset
        if isinstance(_nested_join, Unset):
            nested_join = UNSET
        else:
            nested_join = JoinClause.from_dict(_nested_join)

        join_clause = cls(
            right_table=right_table,
            on=on,
            join_type=join_type,
            right_filters=right_filters,
            right_fields=right_fields,
            strategy_hint=strategy_hint,
            nested_join=nested_join,
        )

        join_clause.additional_properties = d
        return join_clause

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
