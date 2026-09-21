from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionAttributeGroup")


@_attrs_define
class ExtractionAttributeGroup:
    """Version 2 attributes are scored on retained entity spans using shared encoded states. Omitted applies_to selects all
    entities; [] selects none. Raw labels must be unique across groups, including when qualify_labels is true. Group
    names text,confidence,start,end are reserved.

        Attributes:
            labels (list[str]):
            multi_label (bool | Unset):  Default: False.
            threshold (float | Unset):
            applies_to (list[str] | Unset):
            qualify_labels (bool | Unset):  Default: False.
    """

    labels: list[str]
    multi_label: bool | Unset = False
    threshold: float | Unset = UNSET
    applies_to: list[str] | Unset = UNSET
    qualify_labels: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        labels = self.labels

        multi_label = self.multi_label

        threshold = self.threshold

        applies_to: list[str] | Unset = UNSET
        if not isinstance(self.applies_to, Unset):
            applies_to = self.applies_to

        qualify_labels = self.qualify_labels

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "labels": labels,
            }
        )
        if multi_label is not UNSET:
            field_dict["multi_label"] = multi_label
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if applies_to is not UNSET:
            field_dict["applies_to"] = applies_to
        if qualify_labels is not UNSET:
            field_dict["qualify_labels"] = qualify_labels

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        labels = cast(list[str], d.pop("labels"))

        multi_label = d.pop("multi_label", UNSET)

        threshold = d.pop("threshold", UNSET)

        applies_to = cast(list[str], d.pop("applies_to", UNSET))

        qualify_labels = d.pop("qualify_labels", UNSET)

        extraction_attribute_group = cls(
            labels=labels,
            multi_label=multi_label,
            threshold=threshold,
            applies_to=applies_to,
            qualify_labels=qualify_labels,
        )

        return extraction_attribute_group
