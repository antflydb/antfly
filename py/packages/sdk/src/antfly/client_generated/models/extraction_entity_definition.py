from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_entity_definition_dtype import ExtractionEntityDefinitionDtype
from ..models.extraction_entity_definition_type import ExtractionEntityDefinitionType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_regex_validator import ExtractionRegexValidator


T = TypeVar("T", bound="ExtractionEntityDefinition")


@_attrs_define
class ExtractionEntityDefinition:
    """
    Attributes:
        description (str | Unset):
        dtype (ExtractionEntityDefinitionDtype | Unset):
        type_ (ExtractionEntityDefinitionType | Unset):
        threshold (float | Unset):
        validators (list[ExtractionRegexValidator] | Unset):
    """

    description: str | Unset = UNSET
    dtype: ExtractionEntityDefinitionDtype | Unset = UNSET
    type_: ExtractionEntityDefinitionType | Unset = UNSET
    threshold: float | Unset = UNSET
    validators: list[ExtractionRegexValidator] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        description = self.description

        dtype: str | Unset = UNSET
        if not isinstance(self.dtype, Unset):
            dtype = self.dtype.value

        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        threshold = self.threshold

        validators: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.validators, Unset):
            validators = []
            for validators_item_data in self.validators:
                validators_item = validators_item_data.to_dict()
                validators.append(validators_item)

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if description is not UNSET:
            field_dict["description"] = description
        if dtype is not UNSET:
            field_dict["dtype"] = dtype
        if type_ is not UNSET:
            field_dict["type"] = type_
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if validators is not UNSET:
            field_dict["validators"] = validators

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_regex_validator import ExtractionRegexValidator

        d = dict(src_dict)
        description = d.pop("description", UNSET)

        _dtype = d.pop("dtype", UNSET)
        dtype: ExtractionEntityDefinitionDtype | Unset
        if isinstance(_dtype, Unset):
            dtype = UNSET
        else:
            dtype = ExtractionEntityDefinitionDtype(_dtype)

        _type_ = d.pop("type", UNSET)
        type_: ExtractionEntityDefinitionType | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = ExtractionEntityDefinitionType(_type_)

        threshold = d.pop("threshold", UNSET)

        _validators = d.pop("validators", UNSET)
        validators: list[ExtractionRegexValidator] | Unset = UNSET
        if _validators is not UNSET:
            validators = []
            for validators_item_data in _validators:
                validators_item = ExtractionRegexValidator.from_dict(validators_item_data)

                validators.append(validators_item)

        extraction_entity_definition = cls(
            description=description,
            dtype=dtype,
            type_=type_,
            threshold=threshold,
            validators=validators,
        )

        return extraction_entity_definition
