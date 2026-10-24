from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryOutcome")


@_attrs_define
class MemoryOutcome:
    """
    Attributes:
        id (str):
        task_id (str):
        success (bool):
        used_memory_ids (list[str]):
        detail (str | Unset):
    """

    id: str
    task_id: str
    success: bool
    used_memory_ids: list[str]
    detail: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        task_id = self.task_id

        success = self.success

        used_memory_ids = self.used_memory_ids

        detail = self.detail

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "task_id": task_id,
                "success": success,
                "used_memory_ids": used_memory_ids,
            }
        )
        if detail is not UNSET:
            field_dict["detail"] = detail

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        task_id = d.pop("task_id")

        success = d.pop("success")

        used_memory_ids = cast(list[str], d.pop("used_memory_ids"))

        detail = d.pop("detail", UNSET)

        memory_outcome = cls(
            id=id,
            task_id=task_id,
            success=success,
            used_memory_ids=used_memory_ids,
            detail=detail,
        )

        return memory_outcome
