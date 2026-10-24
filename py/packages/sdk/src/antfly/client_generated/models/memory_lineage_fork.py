from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_lineage_fork_visibility import MemoryLineageForkVisibility
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryLineageFork")


@_attrs_define
class MemoryLineageFork:
    """
    Attributes:
        id (str):
        expected_revision (int):
        from_ (str | Unset):
        checkpoint (None | str | Unset):
        visibility (MemoryLineageForkVisibility | Unset):  Default: MemoryLineageForkVisibility.PRIVATE.
    """

    id: str
    expected_revision: int
    from_: str | Unset = UNSET
    checkpoint: None | str | Unset = UNSET
    visibility: MemoryLineageForkVisibility | Unset = MemoryLineageForkVisibility.PRIVATE

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        expected_revision = self.expected_revision

        from_ = self.from_

        checkpoint: None | str | Unset
        if isinstance(self.checkpoint, Unset):
            checkpoint = UNSET
        else:
            checkpoint = self.checkpoint

        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "expected_revision": expected_revision,
            }
        )
        if from_ is not UNSET:
            field_dict["from"] = from_
        if checkpoint is not UNSET:
            field_dict["checkpoint"] = checkpoint
        if visibility is not UNSET:
            field_dict["visibility"] = visibility

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        expected_revision = d.pop("expected_revision")

        from_ = d.pop("from", UNSET)

        def _parse_checkpoint(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        checkpoint = _parse_checkpoint(d.pop("checkpoint", UNSET))

        _visibility = d.pop("visibility", UNSET)
        visibility: MemoryLineageForkVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = MemoryLineageForkVisibility(_visibility)

        memory_lineage_fork = cls(
            id=id,
            expected_revision=expected_revision,
            from_=from_,
            checkpoint=checkpoint,
            visibility=visibility,
        )

        return memory_lineage_fork
