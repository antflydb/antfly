from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.extraction_constraint_min_level_type import ExtractionConstraintMinLevelType

T = TypeVar("T", bound="ExtractionConstraintMinLevel")


@_attrs_define
class ExtractionConstraintMinLevel:
    """
    Attributes:
        type_ (ExtractionConstraintMinLevelType):
        task (str):
        level (int | str):
    """

    type_: ExtractionConstraintMinLevelType
    task: str
    level: int | str

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        task = self.task

        level: int | str
        level = self.level

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "task": task,
                "level": level,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionConstraintMinLevelType(d.pop("type"))

        task = d.pop("task")

        def _parse_level(data: object) -> int | str:
            return cast(int | str, data)

        level = _parse_level(d.pop("level"))

        extraction_constraint_min_level = cls(
            type_=type_,
            task=task,
            level=level,
        )

        return extraction_constraint_min_level
