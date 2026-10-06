from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_index_key import RelationalIndexKey
    from ..models.relational_index_predicate import RelationalIndexPredicate


T = TypeVar("T", bound="RelationalIndexDefinition")


@_attrs_define
class RelationalIndexDefinition:
    """Declarative table-owned ordered index. Keys are compared lexicographically
    in the declared order, with independent direction, null placement, and
    string collation. Creation builds existing rows asynchronously; queries
    must wait for range-local coverage. Unique constraints and expression
    keys are not implied by this object. Optional WHERE conjuncts select
    only matching rows. INCLUDE columns
    store typed values alongside keys for index-only projected reads.

        Attributes:
            name (str):
            keys (list[RelationalIndexKey]):
            description (str | Unset): Optional human-readable description, also exposed by the shared indexes API.
            include_columns (list[str] | Unset): Non-key columns stored in the index; must be distinct from key columns.
            where (list[RelationalIndexPredicate] | Unset): Conjunction of typed predicates. Indexed queries must explicitly
                contain every conjunct with equivalent typed comparison semantics.
    """

    name: str
    keys: list[RelationalIndexKey]
    description: str | Unset = UNSET
    include_columns: list[str] | Unset = UNSET
    where: list[RelationalIndexPredicate] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        keys = []
        for keys_item_data in self.keys:
            keys_item = keys_item_data.to_dict()
            keys.append(keys_item)

        description = self.description

        include_columns: list[str] | Unset = UNSET
        if not isinstance(self.include_columns, Unset):
            include_columns = self.include_columns

        where: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = []
            for where_item_data in self.where:
                where_item = where_item_data.to_dict()
                where.append(where_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "keys": keys,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if include_columns is not UNSET:
            field_dict["include_columns"] = include_columns
        if where is not UNSET:
            field_dict["where"] = where

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_index_key import RelationalIndexKey
        from ..models.relational_index_predicate import RelationalIndexPredicate

        d = dict(src_dict)
        name = d.pop("name")

        keys = []
        _keys = d.pop("keys")
        for keys_item_data in _keys:
            keys_item = RelationalIndexKey.from_dict(keys_item_data)

            keys.append(keys_item)

        description = d.pop("description", UNSET)

        include_columns = cast(list[str], d.pop("include_columns", UNSET))

        _where = d.pop("where", UNSET)
        where: list[RelationalIndexPredicate] | Unset = UNSET
        if _where is not UNSET:
            where = []
            for where_item_data in _where:
                where_item = RelationalIndexPredicate.from_dict(where_item_data)

                where.append(where_item)

        relational_index_definition = cls(
            name=name,
            keys=keys,
            description=description,
            include_columns=include_columns,
            where=where,
        )

        return relational_index_definition
