from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_constraint_label_ref_type import ExtractionConstraintLabelRefType

T = TypeVar("T", bound="ExtractionConstraintLabelRef")


@_attrs_define
class ExtractionConstraintLabelRef:
    """
    Attributes:
        type_ (ExtractionConstraintLabelRefType):
        task (str):
        label (str):
    """

    type_: ExtractionConstraintLabelRefType
    task: str
    label: str

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        task = self.task

        label = self.label

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "task": task,
                "label": label,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionConstraintLabelRefType(d.pop("type"))

        task = d.pop("task")

        label = d.pop("label")

        extraction_constraint_label_ref = cls(
            type_=type_,
            task=task,
            label=label,
        )

        return extraction_constraint_label_ref
