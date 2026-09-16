from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_constraint_any_selected_type import ExtractionConstraintAnySelectedType

T = TypeVar("T", bound="ExtractionConstraintAnySelected")


@_attrs_define
class ExtractionConstraintAnySelected:
    """
    Attributes:
        type_ (ExtractionConstraintAnySelectedType):
        task (str):
    """

    type_: ExtractionConstraintAnySelectedType
    task: str

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        task = self.task

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "task": task,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionConstraintAnySelectedType(d.pop("type"))

        task = d.pop("task")

        extraction_constraint_any_selected = cls(
            type_=type_,
            task=task,
        )

        return extraction_constraint_any_selected
