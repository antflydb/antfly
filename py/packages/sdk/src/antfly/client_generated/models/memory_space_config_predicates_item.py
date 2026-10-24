from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_space_config_predicates_item_cardinality import MemorySpaceConfigPredicatesItemCardinality
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemorySpaceConfigPredicatesItem")


@_attrs_define
class MemorySpaceConfigPredicatesItem:
    """
    Attributes:
        name (str):
        cardinality (MemorySpaceConfigPredicatesItemCardinality | Unset):  Default:
            MemorySpaceConfigPredicatesItemCardinality.MANY.
        subject_type (None | str | Unset):
        object_type (None | str | Unset):
    """

    name: str
    cardinality: MemorySpaceConfigPredicatesItemCardinality | Unset = MemorySpaceConfigPredicatesItemCardinality.MANY
    subject_type: None | str | Unset = UNSET
    object_type: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        cardinality: str | Unset = UNSET
        if not isinstance(self.cardinality, Unset):
            cardinality = self.cardinality.value

        subject_type: None | str | Unset
        if isinstance(self.subject_type, Unset):
            subject_type = UNSET
        else:
            subject_type = self.subject_type

        object_type: None | str | Unset
        if isinstance(self.object_type, Unset):
            object_type = UNSET
        else:
            object_type = self.object_type

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
            }
        )
        if cardinality is not UNSET:
            field_dict["cardinality"] = cardinality
        if subject_type is not UNSET:
            field_dict["subject_type"] = subject_type
        if object_type is not UNSET:
            field_dict["object_type"] = object_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        _cardinality = d.pop("cardinality", UNSET)
        cardinality: MemorySpaceConfigPredicatesItemCardinality | Unset
        if isinstance(_cardinality, Unset):
            cardinality = UNSET
        else:
            cardinality = MemorySpaceConfigPredicatesItemCardinality(_cardinality)

        def _parse_subject_type(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        subject_type = _parse_subject_type(d.pop("subject_type", UNSET))

        def _parse_object_type(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        object_type = _parse_object_type(d.pop("object_type", UNSET))

        memory_space_config_predicates_item = cls(
            name=name,
            cardinality=cardinality,
            subject_type=subject_type,
            object_type=object_type,
        )

        return memory_space_config_predicates_item
