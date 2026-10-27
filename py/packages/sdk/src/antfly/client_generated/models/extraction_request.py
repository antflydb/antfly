from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_schema_version import ExtractionSchemaVersion
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_source import DocumentSource
    from ..models.extraction_input import ExtractionInput
    from ..models.extraction_options import ExtractionOptions
    from ..models.extraction_schema import ExtractionSchema


T = TypeVar("T", bound="ExtractionRequest")


@_attrs_define
class ExtractionRequest:
    """Atomic extraction request. Supply exactly one of inputs, documents or read_artifact. Every input is validated before
    inference; failures return no partial data.

        Attributes:
            model (str):
            schema (ExtractionSchema): Version 1 selects one extraction family; entities may accompany relations.
                With schema_version 2, entities, attributes, classifications, structures,
                and ordinary relations may share one encoded input. joint_ie is a separate,
                mutually exclusive typed graph schema. The version 2 compiler rejects
                unknown fields and validates all references before model execution.
            schema_version (ExtractionSchemaVersion | Unset): Omission preserves the legacy extraction contract. Version 2
                opts into strict mixed-task schemas, per-input replacements and explicit offsets; the selected model/runtime
                must support every requested feature.
            read_artifact (str | Unset): Opaque handle returned by read with retain_artifact. Mutually exclusive with inputs
                and documents. Reuses the immutable complete reading without downloading or running OCR again. Handles expire
                and are local to the serving process.
            documents (list[DocumentSource] | Unset): Complete PDF/PPTX/image sources. Mutually exclusive with inputs.
                Reading and extraction share document adapters and preserve independent source grouping.
            inputs (list[ExtractionInput] | Unset):
            options (ExtractionOptions | Unset):
    """

    model: str
    schema: ExtractionSchema
    schema_version: ExtractionSchemaVersion | Unset = UNSET
    read_artifact: str | Unset = UNSET
    documents: list[DocumentSource] | Unset = UNSET
    inputs: list[ExtractionInput] | Unset = UNSET
    options: ExtractionOptions | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        schema = self.schema.to_dict()

        schema_version: int | Unset = UNSET
        if not isinstance(self.schema_version, Unset):
            schema_version = self.schema_version.value

        read_artifact = self.read_artifact

        documents: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.documents, Unset):
            documents = []
            for documents_item_data in self.documents:
                documents_item = documents_item_data.to_dict()
                documents.append(documents_item)

        inputs: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.inputs, Unset):
            inputs = []
            for inputs_item_data in self.inputs:
                inputs_item = inputs_item_data.to_dict()
                inputs.append(inputs_item)

        options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.options, Unset):
            options = self.options.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "schema": schema,
            }
        )
        if schema_version is not UNSET:
            field_dict["schema_version"] = schema_version
        if read_artifact is not UNSET:
            field_dict["read_artifact"] = read_artifact
        if documents is not UNSET:
            field_dict["documents"] = documents
        if inputs is not UNSET:
            field_dict["inputs"] = inputs
        if options is not UNSET:
            field_dict["options"] = options

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_source import DocumentSource
        from ..models.extraction_input import ExtractionInput
        from ..models.extraction_options import ExtractionOptions
        from ..models.extraction_schema import ExtractionSchema

        d = dict(src_dict)
        model = d.pop("model")

        schema = ExtractionSchema.from_dict(d.pop("schema"))

        _schema_version = d.pop("schema_version", UNSET)
        schema_version: ExtractionSchemaVersion | Unset
        if isinstance(_schema_version, Unset):
            schema_version = UNSET
        else:
            schema_version = ExtractionSchemaVersion(_schema_version)

        read_artifact = d.pop("read_artifact", UNSET)

        _documents = d.pop("documents", UNSET)
        documents: list[DocumentSource] | Unset = UNSET
        if _documents is not UNSET:
            documents = []
            for documents_item_data in _documents:
                documents_item = DocumentSource.from_dict(documents_item_data)

                documents.append(documents_item)

        _inputs = d.pop("inputs", UNSET)
        inputs: list[ExtractionInput] | Unset = UNSET
        if _inputs is not UNSET:
            inputs = []
            for inputs_item_data in _inputs:
                inputs_item = ExtractionInput.from_dict(inputs_item_data)

                inputs.append(inputs_item)

        _options = d.pop("options", UNSET)
        options: ExtractionOptions | Unset
        if isinstance(_options, Unset):
            options = UNSET
        else:
            options = ExtractionOptions.from_dict(_options)

        extraction_request = cls(
            model=model,
            schema=schema,
            schema_version=schema_version,
            read_artifact=read_artifact,
            documents=documents,
            inputs=inputs,
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
