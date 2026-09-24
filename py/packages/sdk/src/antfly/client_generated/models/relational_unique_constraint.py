from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.foreign_key_timing import ForeignKeyTiming
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_index_key import RelationalIndexKey
    from ..models.relational_index_predicate import RelationalIndexPredicate


T = TypeVar("T", bound="RelationalUniqueConstraint")


@_attrs_define
class RelationalUniqueConstraint:
    """A named, ordered composite unique key. Validation status is maintained
    by the server. TTL expiry uses the distributed integrity coordinator.
    Referenced unique keys are nondeferrable.

        Attributes:
            name (str):
            primary (bool | Unset): SQL primary-key identity. At most one per relational table; all key columns must be
                required and nonnullable.
            columns (list[str] | Unset):
            keys (list[RelationalIndexKey] | Unset): Typed native unique keys. Specify either columns or keys.
            where (list[RelationalIndexPredicate] | Unset): Conjunction restricting uniqueness to matching rows.
            nulls_not_distinct (bool | Unset): When true, NULL components compare equal for uniqueness.
            deferrable (bool | Unset): Permit uniqueness checks at transaction commit. Never eligible as an ON CONFLICT
                arbiter or a referenced foreign key target.
            timing (ForeignKeyTiming | Unset): Enforcement timing for atomic mutations and transaction sessions. Deferred
                requires deferrable=true and validates the final transaction state.
                NO ACTION permits a valid final-state parent replacement; RESTRICT
                still rejects referenced parent removal. Existing multi-request
                transaction sessions retain deferred checks until commit; immediate
                checks apply to each staged statement. SET CONSTRAINTS is not provided.
    """

    name: str
    primary: bool | Unset = UNSET
    columns: list[str] | Unset = UNSET
    keys: list[RelationalIndexKey] | Unset = UNSET
    where: list[RelationalIndexPredicate] | Unset = UNSET
    nulls_not_distinct: bool | Unset = UNSET
    deferrable: bool | Unset = UNSET
    timing: ForeignKeyTiming | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        primary = self.primary

        columns: list[str] | Unset = UNSET
        if not isinstance(self.columns, Unset):
            columns = self.columns

        keys: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.keys, Unset):
            keys = []
            for keys_item_data in self.keys:
                keys_item = keys_item_data.to_dict()
                keys.append(keys_item)

        where: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = []
            for where_item_data in self.where:
                where_item = where_item_data.to_dict()
                where.append(where_item)

        nulls_not_distinct = self.nulls_not_distinct

        deferrable = self.deferrable

        timing: str | Unset = UNSET
        if not isinstance(self.timing, Unset):
            timing = self.timing.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
            }
        )
        if primary is not UNSET:
            field_dict["primary"] = primary
        if columns is not UNSET:
            field_dict["columns"] = columns
        if keys is not UNSET:
            field_dict["keys"] = keys
        if where is not UNSET:
            field_dict["where"] = where
        if nulls_not_distinct is not UNSET:
            field_dict["nulls_not_distinct"] = nulls_not_distinct
        if deferrable is not UNSET:
            field_dict["deferrable"] = deferrable
        if timing is not UNSET:
            field_dict["timing"] = timing

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_index_key import RelationalIndexKey
        from ..models.relational_index_predicate import RelationalIndexPredicate

        d = dict(src_dict)
        name = d.pop("name")

        primary = d.pop("primary", UNSET)

        columns = cast(list[str], d.pop("columns", UNSET))

        _keys = d.pop("keys", UNSET)
        keys: list[RelationalIndexKey] | Unset = UNSET
        if _keys is not UNSET:
            keys = []
            for keys_item_data in _keys:
                keys_item = RelationalIndexKey.from_dict(keys_item_data)

                keys.append(keys_item)

        _where = d.pop("where", UNSET)
        where: list[RelationalIndexPredicate] | Unset = UNSET
        if _where is not UNSET:
            where = []
            for where_item_data in _where:
                where_item = RelationalIndexPredicate.from_dict(where_item_data)

                where.append(where_item)

        nulls_not_distinct = d.pop("nulls_not_distinct", UNSET)

        deferrable = d.pop("deferrable", UNSET)

        _timing = d.pop("timing", UNSET)
        timing: ForeignKeyTiming | Unset
        if isinstance(_timing, Unset):
            timing = UNSET
        else:
            timing = ForeignKeyTiming(_timing)

        relational_unique_constraint = cls(
            name=name,
            primary=primary,
            columns=columns,
            keys=keys,
            where=where,
            nulls_not_distinct=nulls_not_distinct,
            deferrable=deferrable,
            timing=timing,
        )

        return relational_unique_constraint
