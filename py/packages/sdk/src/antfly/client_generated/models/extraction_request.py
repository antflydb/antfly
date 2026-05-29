from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_input import ExtractionInput
    from ..models.extraction_options import ExtractionOptions
    from ..models.extraction_schema import ExtractionSchema


T = TypeVar("T", bound="ExtractionRequest")


@_attrs_define
class ExtractionRequest:
    """
    Attributes:
        model (str):
        inputs (list[ExtractionInput]):
        schema (ExtractionSchema):
        options (ExtractionOptions | Unset):
    """

    model: str
    inputs: list[ExtractionInput]
    schema: ExtractionSchema
    options: ExtractionOptions | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        inputs = []
        for inputs_item_data in self.inputs:
            inputs_item = inputs_item_data.to_dict()
            inputs.append(inputs_item)

        schema = self.schema.to_dict()

        options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.options, Unset):
            options = self.options.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "inputs": inputs,
                "schema": schema,
            }
        )
        if options is not UNSET:
            field_dict["options"] = options

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_input import ExtractionInput
        from ..models.extraction_options import ExtractionOptions
        from ..models.extraction_schema import ExtractionSchema

        d = dict(src_dict)
        model = d.pop("model")

        inputs = []
        _inputs = d.pop("inputs")
        for inputs_item_data in _inputs:
            inputs_item = ExtractionInput.from_dict(inputs_item_data)

            inputs.append(inputs_item)

        schema = ExtractionSchema.from_dict(d.pop("schema"))

        _options = d.pop("options", UNSET)
        options: ExtractionOptions | Unset
        if isinstance(_options, Unset):
            options = UNSET
        else:
            options = ExtractionOptions.from_dict(_options)

        extraction_request = cls(
            model=model,
            inputs=inputs,
            schema=schema,
            options=options,
        )

        extraction_request.additional_properties = d
        return extraction_request

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
