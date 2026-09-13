from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.relational_index_key import RelationalIndexKey


T = TypeVar("T", bound="RelationalIndexDefinition")


@_attrs_define
class RelationalIndexDefinition:
    """Declarative table-owned ordered index. Keys are compared lexicographically
    in the declared order, with independent direction, null placement, and
    string collation. Creation builds existing rows asynchronously; queries
    must wait for range-local coverage. Unique constraints, expressions,
    partial predicates, and covering payloads are not implied by this object.

        Attributes:
            name (str):
            keys (list[RelationalIndexKey]):
    """

    name: str
    keys: list[RelationalIndexKey]

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        keys = []
        for keys_item_data in self.keys:
            keys_item = keys_item_data.to_dict()
            keys.append(keys_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "keys": keys,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_index_key import RelationalIndexKey

        d = dict(src_dict)
        name = d.pop("name")

        keys = []
        _keys = d.pop("keys")
        for keys_item_data in _keys:
            keys_item = RelationalIndexKey.from_dict(keys_item_data)

            keys.append(keys_item)

        relational_index_definition = cls(
            name=name,
            keys=keys,
        )

        return relational_index_definition
