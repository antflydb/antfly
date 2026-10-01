from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.extraction_constraint_cardinality_type import ExtractionConstraintCardinalityType
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionConstraintCardinality")


@_attrs_define
class ExtractionConstraintCardinality:
    """
    Attributes:
        type_ (ExtractionConstraintCardinalityType):
        task (str):
        minimum (int | Unset):
        maximum (int | None | Unset):
    """

    type_: ExtractionConstraintCardinalityType
    task: str
    minimum: int | Unset = UNSET
    maximum: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        task = self.task

        minimum = self.minimum

        maximum: int | None | Unset
        if isinstance(self.maximum, Unset):
            maximum = UNSET
        else:
            maximum = self.maximum

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "task": task,
            }
        )
        if minimum is not UNSET:
            field_dict["minimum"] = minimum
        if maximum is not UNSET:
            field_dict["maximum"] = maximum

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionConstraintCardinalityType(d.pop("type"))

        task = d.pop("task")

        minimum = d.pop("minimum", UNSET)

        def _parse_maximum(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        maximum = _parse_maximum(d.pop("maximum", UNSET))

        extraction_constraint_cardinality = cls(
            type_=type_,
            task=task,
            minimum=minimum,
            maximum=maximum,
        )

        return extraction_constraint_cardinality
