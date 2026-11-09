from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_brief_item import MemoryBriefItem
    from ..models.memory_subject_count import MemorySubjectCount


T = TypeVar("T", bound="MemoryBrief")


@_attrs_define
class MemoryBrief:
    """
    Attributes:
        items (list[MemoryBriefItem]):
        subjects (list[MemorySubjectCount]):
        guidance (str):
        scanned (int):
        omitted (int):
        complete (bool):
        token_upper_bound (int):
        revision (int):
        config_generation (int):
    """

    items: list[MemoryBriefItem]
    subjects: list[MemorySubjectCount]
    guidance: str
    scanned: int
    omitted: int
    complete: bool
    token_upper_bound: int
    revision: int
    config_generation: int

    def to_dict(self) -> dict[str, Any]:
        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        subjects = []
        for subjects_item_data in self.subjects:
            subjects_item = subjects_item_data.to_dict()
            subjects.append(subjects_item)

        guidance = self.guidance

        scanned = self.scanned

        omitted = self.omitted

        complete = self.complete

        token_upper_bound = self.token_upper_bound

        revision = self.revision

        config_generation = self.config_generation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "items": items,
                "subjects": subjects,
                "guidance": guidance,
                "scanned": scanned,
                "omitted": omitted,
                "complete": complete,
                "token_upper_bound": token_upper_bound,
                "revision": revision,
                "config_generation": config_generation,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_brief_item import MemoryBriefItem
        from ..models.memory_subject_count import MemorySubjectCount

        d = dict(src_dict)
        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = MemoryBriefItem.from_dict(items_item_data)

            items.append(items_item)

        subjects = []
        _subjects = d.pop("subjects")
        for subjects_item_data in _subjects:
            subjects_item = MemorySubjectCount.from_dict(subjects_item_data)

            subjects.append(subjects_item)

        guidance = d.pop("guidance")

        scanned = d.pop("scanned")

        omitted = d.pop("omitted")

        complete = d.pop("complete")

        token_upper_bound = d.pop("token_upper_bound")

        revision = d.pop("revision")

        config_generation = d.pop("config_generation")

        memory_brief = cls(
            items=items,
            subjects=subjects,
            guidance=guidance,
            scanned=scanned,
            omitted=omitted,
            complete=complete,
            token_upper_bound=token_upper_bound,
            revision=revision,
            config_generation=config_generation,
        )

        return memory_brief
