from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.composed_table_source import ComposedTableSource


T = TypeVar("T", bound="ComposedSourceOverlay")


@_attrs_define
class ComposedSourceOverlay:
    """
    Attributes:
        base (ComposedTableSource):
        changes (ComposedTableSource):
        key (list[str]):
        tombstone_field (str | Unset): Boolean field on change rows; true hides the base row. Changes must retain one
            latest row/tombstone per stable key. Row-policy identities are currently unsupported for keyed composition.
            Default: 'deleted'.
    """

    base: ComposedTableSource
    changes: ComposedTableSource
    key: list[str]
    tombstone_field: str | Unset = "deleted"

    def to_dict(self) -> dict[str, Any]:
        base = self.base.to_dict()

        changes = self.changes.to_dict()

        key = self.key

        tombstone_field = self.tombstone_field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "base": base,
                "changes": changes,
                "key": key,
            }
        )
        if tombstone_field is not UNSET:
            field_dict["tombstone_field"] = tombstone_field

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.composed_table_source import ComposedTableSource

        d = dict(src_dict)
        base = ComposedTableSource.from_dict(d.pop("base"))

        changes = ComposedTableSource.from_dict(d.pop("changes"))

        key = cast(list[str], d.pop("key"))

        tombstone_field = d.pop("tombstone_field", UNSET)

        composed_source_overlay = cls(
            base=base,
            changes=changes,
            key=key,
            tombstone_field=tombstone_field,
        )

        return composed_source_overlay
