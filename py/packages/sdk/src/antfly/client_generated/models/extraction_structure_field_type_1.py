from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_structure_field_type_1_cardinality import ExtractionStructureFieldType1Cardinality
from ..models.extraction_structure_field_type_1_dtype import ExtractionStructureFieldType1Dtype
from ..models.extraction_structure_field_type_1_type import ExtractionStructureFieldType1Type
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_regex_validator import ExtractionRegexValidator


T = TypeVar("T", bound="ExtractionStructureFieldType1")


@_attrs_define
class ExtractionStructureFieldType1:
    """
    Attributes:
        type_ (ExtractionStructureFieldType1Type | Unset):
        enum (list[str] | Unset):
        dtype (ExtractionStructureFieldType1Dtype | Unset):
        choices (list[str] | Unset):
        description (str | Unset):
        threshold (float | Unset):
        cardinality (ExtractionStructureFieldType1Cardinality | Unset): Version 2 explicit cardinality. Required fields
            are validated after record assignment.
        exclusive (bool | Unset): Version 2 field spans cannot be assigned to multiple record instances.
        validators (list[ExtractionRegexValidator] | Unset):
    """

    type_: ExtractionStructureFieldType1Type | Unset = UNSET
    enum: list[str] | Unset = UNSET
    dtype: ExtractionStructureFieldType1Dtype | Unset = UNSET
    choices: list[str] | Unset = UNSET
    description: str | Unset = UNSET
    threshold: float | Unset = UNSET
    cardinality: ExtractionStructureFieldType1Cardinality | Unset = UNSET
    exclusive: bool | Unset = UNSET
    validators: list[ExtractionRegexValidator] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        enum: list[str] | Unset = UNSET
        if not isinstance(self.enum, Unset):
            enum = self.enum

        dtype: str | Unset = UNSET
        if not isinstance(self.dtype, Unset):
            dtype = self.dtype.value

        choices: list[str] | Unset = UNSET
        if not isinstance(self.choices, Unset):
            choices = self.choices

        description = self.description

        threshold = self.threshold

        cardinality: str | Unset = UNSET
        if not isinstance(self.cardinality, Unset):
            cardinality = self.cardinality.value

        exclusive = self.exclusive

        validators: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.validators, Unset):
            validators = []
            for validators_item_data in self.validators:
                validators_item = validators_item_data.to_dict()
                validators.append(validators_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if type_ is not UNSET:
            field_dict["type"] = type_
        if enum is not UNSET:
            field_dict["enum"] = enum
        if dtype is not UNSET:
            field_dict["dtype"] = dtype
        if choices is not UNSET:
            field_dict["choices"] = choices
        if description is not UNSET:
            field_dict["description"] = description
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if cardinality is not UNSET:
            field_dict["cardinality"] = cardinality
        if exclusive is not UNSET:
            field_dict["exclusive"] = exclusive
        if validators is not UNSET:
            field_dict["validators"] = validators

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_regex_validator import ExtractionRegexValidator

        d = dict(src_dict)
        _type_ = d.pop("type", UNSET)
        type_: ExtractionStructureFieldType1Type | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = ExtractionStructureFieldType1Type(_type_)

        enum = cast(list[str], d.pop("enum", UNSET))

        _dtype = d.pop("dtype", UNSET)
        dtype: ExtractionStructureFieldType1Dtype | Unset
        if isinstance(_dtype, Unset):
            dtype = UNSET
        else:
            dtype = ExtractionStructureFieldType1Dtype(_dtype)

        choices = cast(list[str], d.pop("choices", UNSET))

        description = d.pop("description", UNSET)

        threshold = d.pop("threshold", UNSET)

        _cardinality = d.pop("cardinality", UNSET)
        cardinality: ExtractionStructureFieldType1Cardinality | Unset
        if isinstance(_cardinality, Unset):
            cardinality = UNSET
        else:
            cardinality = ExtractionStructureFieldType1Cardinality(_cardinality)

        exclusive = d.pop("exclusive", UNSET)

        _validators = d.pop("validators", UNSET)
        validators: list[ExtractionRegexValidator] | Unset = UNSET
        if _validators is not UNSET:
            validators = []
            for validators_item_data in _validators:
                validators_item = ExtractionRegexValidator.from_dict(validators_item_data)

                validators.append(validators_item)

        extraction_structure_field_type_1 = cls(
            type_=type_,
            enum=enum,
            dtype=dtype,
            choices=choices,
            description=description,
            threshold=threshold,
            cardinality=cardinality,
            exclusive=exclusive,
            validators=validators,
        )

        extraction_structure_field_type_1.additional_properties = d
        return extraction_structure_field_type_1

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
