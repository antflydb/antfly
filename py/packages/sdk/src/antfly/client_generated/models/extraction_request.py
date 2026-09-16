from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_schema_version import ExtractionSchemaVersion
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_input import ExtractionInput
    from ..models.extraction_options import ExtractionOptions
    from ..models.extraction_schema import ExtractionSchema


T = TypeVar("T", bound="ExtractionRequest")


@_attrs_define
class ExtractionRequest:
    """Atomic extraction request. Every input is validated before inference; failures return no partial data.

    Attributes:
        model (str):
        inputs (list[ExtractionInput]):
        schema (ExtractionSchema): Version 1 selects one extraction family; entities may accompany relations.
            With schema_version 2, entities, attributes, classifications, structures,
            and ordinary relations may share one encoded input. joint_ie is a separate,
            mutually exclusive typed graph schema. The version 2 compiler rejects
            unknown fields and validates all references before model execution.
        schema_version (ExtractionSchemaVersion | Unset): Omission preserves the legacy extraction contract. Version 2
            opts into strict mixed-task schemas, per-input replacements and explicit offsets; the selected model/runtime
            must support every requested feature.
        options (ExtractionOptions | Unset):
    """

    model: str
    inputs: list[ExtractionInput]
    schema: ExtractionSchema
    schema_version: ExtractionSchemaVersion | Unset = UNSET
    options: ExtractionOptions | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        inputs = []
        for inputs_item_data in self.inputs:
            inputs_item = inputs_item_data.to_dict()
            inputs.append(inputs_item)

        schema = self.schema.to_dict()

        schema_version: int | Unset = UNSET
        if not isinstance(self.schema_version, Unset):
            schema_version = self.schema_version.value

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
        if schema_version is not UNSET:
            field_dict["schema_version"] = schema_version
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

        _schema_version = d.pop("schema_version", UNSET)
        schema_version: ExtractionSchemaVersion | Unset
        if isinstance(_schema_version, Unset):
            schema_version = UNSET
        else:
            schema_version = ExtractionSchemaVersion(_schema_version)

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
            schema_version=schema_version,
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
