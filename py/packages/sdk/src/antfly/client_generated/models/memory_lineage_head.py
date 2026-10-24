from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_lineage_head_visibility import MemoryLineageHeadVisibility
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryLineageHead")


@_attrs_define
class MemoryLineageHead:
    """
    Attributes:
        id (str):
        revision (int):
        owner (str):
        created_at (int):
        root (None | str | Unset):
        visibility (MemoryLineageHeadVisibility | Unset):  Default: MemoryLineageHeadVisibility.PRIVATE.
        parent (None | str | Unset):
        inherited_revision (int | None | Unset):
        complete (bool | Unset):  Default: True.
        cursor (None | str | Unset):
        migration_lane (int | Unset):  Default: 0.
    """

    id: str
    revision: int
    owner: str
    created_at: int
    root: None | str | Unset = UNSET
    visibility: MemoryLineageHeadVisibility | Unset = MemoryLineageHeadVisibility.PRIVATE
    parent: None | str | Unset = UNSET
    inherited_revision: int | None | Unset = UNSET
    complete: bool | Unset = True
    cursor: None | str | Unset = UNSET
    migration_lane: int | Unset = 0

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        revision = self.revision

        owner = self.owner

        created_at = self.created_at

        root: None | str | Unset
        if isinstance(self.root, Unset):
            root = UNSET
        else:
            root = self.root

        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        parent: None | str | Unset
        if isinstance(self.parent, Unset):
            parent = UNSET
        else:
            parent = self.parent

        inherited_revision: int | None | Unset
        if isinstance(self.inherited_revision, Unset):
            inherited_revision = UNSET
        else:
            inherited_revision = self.inherited_revision

        complete = self.complete

        cursor: None | str | Unset
        if isinstance(self.cursor, Unset):
            cursor = UNSET
        else:
            cursor = self.cursor

        migration_lane = self.migration_lane

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "revision": revision,
                "owner": owner,
                "created_at": created_at,
            }
        )
        if root is not UNSET:
            field_dict["root"] = root
        if visibility is not UNSET:
            field_dict["visibility"] = visibility
        if parent is not UNSET:
            field_dict["parent"] = parent
        if inherited_revision is not UNSET:
            field_dict["inherited_revision"] = inherited_revision
        if complete is not UNSET:
            field_dict["complete"] = complete
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if migration_lane is not UNSET:
            field_dict["migration_lane"] = migration_lane

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        revision = d.pop("revision")

        owner = d.pop("owner")

        created_at = d.pop("created_at")

        def _parse_root(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        root = _parse_root(d.pop("root", UNSET))

        _visibility = d.pop("visibility", UNSET)
        visibility: MemoryLineageHeadVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = MemoryLineageHeadVisibility(_visibility)

        def _parse_parent(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        parent = _parse_parent(d.pop("parent", UNSET))

        def _parse_inherited_revision(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        inherited_revision = _parse_inherited_revision(d.pop("inherited_revision", UNSET))

        complete = d.pop("complete", UNSET)

        def _parse_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        cursor = _parse_cursor(d.pop("cursor", UNSET))

        migration_lane = d.pop("migration_lane", UNSET)

        memory_lineage_head = cls(
            id=id,
            revision=revision,
            owner=owner,
            created_at=created_at,
            root=root,
            visibility=visibility,
            parent=parent,
            inherited_revision=inherited_revision,
            complete=complete,
            cursor=cursor,
            migration_lane=migration_lane,
        )

        return memory_lineage_head
