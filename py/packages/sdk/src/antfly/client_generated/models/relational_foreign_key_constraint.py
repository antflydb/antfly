from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.foreign_key_action import ForeignKeyAction
from ..models.foreign_key_match import ForeignKeyMatch
from ..models.foreign_key_timing import ForeignKeyTiming
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalForeignKeyConstraint")


@_attrs_define
class RelationalForeignKeyConstraint:
    """Composite foreign key. Child and parent columns correspond by position
    and must have the same physical comparison types. The parent columns
    must identify a unique key. Existing-row validation is independent of
    new-write enforcement and is never client-writable.
    Deferred timing, MATCH PARTIAL, and TTL expiry are not supported.
    SET NULL requires every child column to accept explicit NULL.

        Attributes:
            name (str):
            child_columns (list[str]):
            parent_table (str):
            parent_columns (list[str]):
            on_delete (ForeignKeyAction | Unset): Action on referencing rows when a referenced row is changed or removed.
            on_update (ForeignKeyAction | Unset): Action on referencing rows when a referenced row is changed or removed.
            timing (ForeignKeyTiming | Unset): Whether foreign-key enforcement occurs immediately or at transaction commit.
            match (ForeignKeyMatch | Unset): Null matching semantics of a composite foreign key.
            deferrable (bool | Unset):
    """

    name: str
    child_columns: list[str]
    parent_table: str
    parent_columns: list[str]
    on_delete: ForeignKeyAction | Unset = UNSET
    on_update: ForeignKeyAction | Unset = UNSET
    timing: ForeignKeyTiming | Unset = UNSET
    match: ForeignKeyMatch | Unset = UNSET
    deferrable: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        child_columns = self.child_columns

        parent_table = self.parent_table

        parent_columns = self.parent_columns

        on_delete: str | Unset = UNSET
        if not isinstance(self.on_delete, Unset):
            on_delete = self.on_delete.value

        on_update: str | Unset = UNSET
        if not isinstance(self.on_update, Unset):
            on_update = self.on_update.value

        timing: str | Unset = UNSET
        if not isinstance(self.timing, Unset):
            timing = self.timing.value

        match: str | Unset = UNSET
        if not isinstance(self.match, Unset):
            match = self.match.value

        deferrable = self.deferrable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "child_columns": child_columns,
                "parent_table": parent_table,
                "parent_columns": parent_columns,
            }
        )
        if on_delete is not UNSET:
            field_dict["on_delete"] = on_delete
        if on_update is not UNSET:
            field_dict["on_update"] = on_update
        if timing is not UNSET:
            field_dict["timing"] = timing
        if match is not UNSET:
            field_dict["match"] = match
        if deferrable is not UNSET:
            field_dict["deferrable"] = deferrable

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        child_columns = cast(list[str], d.pop("child_columns"))

        parent_table = d.pop("parent_table")

        parent_columns = cast(list[str], d.pop("parent_columns"))

        _on_delete = d.pop("on_delete", UNSET)
        on_delete: ForeignKeyAction | Unset
        if isinstance(_on_delete, Unset):
            on_delete = UNSET
        else:
            on_delete = ForeignKeyAction(_on_delete)

        _on_update = d.pop("on_update", UNSET)
        on_update: ForeignKeyAction | Unset
        if isinstance(_on_update, Unset):
            on_update = UNSET
        else:
            on_update = ForeignKeyAction(_on_update)

        _timing = d.pop("timing", UNSET)
        timing: ForeignKeyTiming | Unset
        if isinstance(_timing, Unset):
            timing = UNSET
        else:
            timing = ForeignKeyTiming(_timing)

        _match = d.pop("match", UNSET)
        match: ForeignKeyMatch | Unset
        if isinstance(_match, Unset):
            match = UNSET
        else:
            match = ForeignKeyMatch(_match)

        deferrable = d.pop("deferrable", UNSET)

        relational_foreign_key_constraint = cls(
            name=name,
            child_columns=child_columns,
            parent_table=parent_table,
            parent_columns=parent_columns,
            on_delete=on_delete,
            on_update=on_update,
            timing=timing,
            match=match,
            deferrable=deferrable,
        )

        return relational_foreign_key_constraint
