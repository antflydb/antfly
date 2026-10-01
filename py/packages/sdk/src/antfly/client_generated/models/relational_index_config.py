from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_index_key import RelationalIndexKey
    from ..models.relational_index_predicate import RelationalIndexPredicate


T = TypeVar("T", bound="RelationalIndexConfig")


@_attrs_define
class RelationalIndexConfig:
    """Schema-bound composite ordered index on a relational table. Keys use stable typed comparison semantics and
    independent direction, null placement, and string collation. Existing rows build asynchronously; indexed queries
    require complete owner coverage. The table schema is the single durable authority for these definitions.

        Attributes:
            keys (list[RelationalIndexKey]):
            include_columns (list[str] | Unset): Non-key columns stored for index-only projection; distinct from keys.
            where (list[RelationalIndexPredicate] | Unset): Optional conjunction selecting index members. Queries must
                explicitly include all typed conjuncts.
    """

    keys: list[RelationalIndexKey]
    include_columns: list[str] | Unset = UNSET
    where: list[RelationalIndexPredicate] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        keys = []
        for keys_item_data in self.keys:
            keys_item = keys_item_data.to_dict()
            keys.append(keys_item)

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
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "keys": keys,
            }
        )
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
        keys = []
        _keys = d.pop("keys")
        for keys_item_data in _keys:
            keys_item = RelationalIndexKey.from_dict(keys_item_data)

            keys.append(keys_item)

        include_columns = cast(list[str], d.pop("include_columns", UNSET))

        _where = d.pop("where", UNSET)
        where: list[RelationalIndexPredicate] | Unset = UNSET
        if _where is not UNSET:
            where = []
            for where_item_data in _where:
                where_item = RelationalIndexPredicate.from_dict(where_item_data)

                where.append(where_item)

        relational_index_config = cls(
            keys=keys,
            include_columns=include_columns,
            where=where,
        )

        relational_index_config.additional_properties = d
        return relational_index_config

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
