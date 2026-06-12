from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_generate_json_schema_config_schema import InferenceGenerateJsonSchemaConfigSchema


T = TypeVar("T", bound="InferenceGenerateJsonSchemaConfig")


@_attrs_define
class InferenceGenerateJsonSchemaConfig:
    """
    Attributes:
        name (str | Unset): Schema name
        strict (bool | Unset): Whether output should strictly follow the schema
        schema (InferenceGenerateJsonSchemaConfigSchema | Unset): JSON Schema object
    """

    name: str | Unset = UNSET
    strict: bool | Unset = UNSET
    schema: InferenceGenerateJsonSchemaConfigSchema | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        strict = self.strict

        schema: dict[str, Any] | Unset = UNSET
        if not isinstance(self.schema, Unset):
            schema = self.schema.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if strict is not UNSET:
            field_dict["strict"] = strict
        if schema is not UNSET:
            field_dict["schema"] = schema

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_generate_json_schema_config_schema import InferenceGenerateJsonSchemaConfigSchema

        d = dict(src_dict)
        name = d.pop("name", UNSET)

        strict = d.pop("strict", UNSET)

        _schema = d.pop("schema", UNSET)
        schema: InferenceGenerateJsonSchemaConfigSchema | Unset
        if isinstance(_schema, Unset):
            schema = UNSET
        else:
            schema = InferenceGenerateJsonSchemaConfigSchema.from_dict(_schema)

        inference_generate_json_schema_config = cls(
            name=name,
            strict=strict,
            schema=schema,
        )

        inference_generate_json_schema_config.additional_properties = d
        return inference_generate_json_schema_config

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
