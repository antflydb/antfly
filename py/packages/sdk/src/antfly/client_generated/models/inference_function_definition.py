from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_function_definition_parameters import InferenceFunctionDefinitionParameters


T = TypeVar("T", bound="InferenceFunctionDefinition")


@_attrs_define
class InferenceFunctionDefinition:
    """Definition of a function that can be called by the model

    Attributes:
        name (str): The name of the function to call Example: get_weather.
        description (str | Unset): A description of what the function does Example: Get the current weather in a
            location.
        parameters (InferenceFunctionDefinitionParameters | Unset): JSON Schema object describing the function
            parameters Example: {'type': 'object', 'properties': {'location': {'type': 'string', 'description': 'The city
            and state, e.g. San Francisco, CA'}}, 'required': ['location']}.
        strict (bool | Unset): Whether to enforce strict parameter validation Default: False.
    """

    name: str
    description: str | Unset = UNSET
    parameters: InferenceFunctionDefinitionParameters | Unset = UNSET
    strict: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        description = self.description

        parameters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = self.parameters.to_dict()

        strict = self.strict

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if parameters is not UNSET:
            field_dict["parameters"] = parameters
        if strict is not UNSET:
            field_dict["strict"] = strict

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_function_definition_parameters import InferenceFunctionDefinitionParameters

        d = dict(src_dict)
        name = d.pop("name")

        description = d.pop("description", UNSET)

        _parameters = d.pop("parameters", UNSET)
        parameters: InferenceFunctionDefinitionParameters | Unset
        if isinstance(_parameters, Unset):
            parameters = UNSET
        else:
            parameters = InferenceFunctionDefinitionParameters.from_dict(_parameters)

        strict = d.pop("strict", UNSET)

        inference_function_definition = cls(
            name=name,
            description=description,
            parameters=parameters,
            strict=strict,
        )

        inference_function_definition.additional_properties = d
        return inference_function_definition

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
