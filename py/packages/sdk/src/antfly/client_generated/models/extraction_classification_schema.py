from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_classification_schema_activation import ExtractionClassificationSchemaActivation
from ..models.extraction_classification_schema_mode import ExtractionClassificationSchemaMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_classification_example_type_0 import ExtractionClassificationExampleType0
    from ..models.extraction_classification_schema_label_definitions import (
        ExtractionClassificationSchemaLabelDefinitions,
    )


T = TypeVar("T", bound="ExtractionClassificationSchema")


@_attrs_define
class ExtractionClassificationSchema:
    """
    Attributes:
        name (str):
        labels (list[str]):
        multi_label (bool | Unset): The server uses false when omitted. Version 1: return highest-ranked labels up to
            top_k when false, or labels meeting options.threshold when true. Version 2: selects ordinary single or multi
            classification unless mode is specified; classification.threshold controls the decision threshold.
        hypothesis_template (str | Unset): Version 1 NLI hypothesis template with {} as the label placeholder; the
            server uses "This example is {}." when omitted. Version 2 GLiNER boundary extraction rejects an explicit
            hypothesis_template; use prompt/instruction and label_definitions for model conditioning.
        top_k (int | Unset): Maximum labels for ordinary single-label classification; the server uses 1 when omitted.
            Version 2 constrained or ordinal selection uses min_labels/max_labels. Advanced set-selection options or cross-
            task constraints on any classification in the collection reject every explicit top_k in that collection,
            including 1. Omit top_k when using these options.
        mode (ExtractionClassificationSchemaMode | Unset): Version 2 classification mode. Ordinal labels are ordered
            from lowest to highest.
        label_definitions (ExtractionClassificationSchemaLabelDefinitions | Unset):
        min_labels (int | Unset):
        max_labels (int | None | Unset): Version 2 maximum selected labels. Explicit null means no maximum; omission
            preserves mode defaults.
        ordered (bool | Unset):
        threshold (float | Unset): Finite centered-logit decisions require a threshold strictly between zero and one.
        candidate_threshold (float | Unset):
        activation (ExtractionClassificationSchemaActivation | Unset):
        temperature (float | Unset):
        default (str | Unset): Version 2 fallback label; must be declared in labels.
        prompt (str | Unset): Version 2 model-facing task instruction. Mutually exclusive with instruction.
        instruction (str | Unset): Alias of prompt.
        examples (list[ExtractionClassificationExampleType0 | list[str]] | Unset):
    """

    name: str
    labels: list[str]
    multi_label: bool | Unset = UNSET
    hypothesis_template: str | Unset = UNSET
    top_k: int | Unset = UNSET
    mode: ExtractionClassificationSchemaMode | Unset = UNSET
    label_definitions: ExtractionClassificationSchemaLabelDefinitions | Unset = UNSET
    min_labels: int | Unset = UNSET
    max_labels: int | None | Unset = UNSET
    ordered: bool | Unset = UNSET
    threshold: float | Unset = UNSET
    candidate_threshold: float | Unset = UNSET
    activation: ExtractionClassificationSchemaActivation | Unset = UNSET
    temperature: float | Unset = UNSET
    default: str | Unset = UNSET
    prompt: str | Unset = UNSET
    instruction: str | Unset = UNSET
    examples: list[ExtractionClassificationExampleType0 | list[str]] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.extraction_classification_example_type_0 import ExtractionClassificationExampleType0

        name = self.name

        labels = self.labels

        multi_label = self.multi_label

        hypothesis_template = self.hypothesis_template

        top_k = self.top_k

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        label_definitions: dict[str, Any] | Unset = UNSET
        if not isinstance(self.label_definitions, Unset):
            label_definitions = self.label_definitions.to_dict()

        min_labels = self.min_labels

        max_labels: int | None | Unset
        if isinstance(self.max_labels, Unset):
            max_labels = UNSET
        else:
            max_labels = self.max_labels

        ordered = self.ordered

        threshold = self.threshold

        candidate_threshold = self.candidate_threshold

        activation: str | Unset = UNSET
        if not isinstance(self.activation, Unset):
            activation = self.activation.value

        temperature = self.temperature

        default = self.default

        prompt = self.prompt

        instruction = self.instruction

        examples: list[dict[str, Any] | list[str]] | Unset = UNSET
        if not isinstance(self.examples, Unset):
            examples = []
            for examples_item_data in self.examples:
                examples_item: dict[str, Any] | list[str]
                if isinstance(examples_item_data, ExtractionClassificationExampleType0):
                    examples_item = examples_item_data.to_dict()
                else:
                    examples_item = examples_item_data

                examples.append(examples_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "labels": labels,
            }
        )
        if multi_label is not UNSET:
            field_dict["multi_label"] = multi_label
        if hypothesis_template is not UNSET:
            field_dict["hypothesis_template"] = hypothesis_template
        if top_k is not UNSET:
            field_dict["top_k"] = top_k
        if mode is not UNSET:
            field_dict["mode"] = mode
        if label_definitions is not UNSET:
            field_dict["label_definitions"] = label_definitions
        if min_labels is not UNSET:
            field_dict["min_labels"] = min_labels
        if max_labels is not UNSET:
            field_dict["max_labels"] = max_labels
        if ordered is not UNSET:
            field_dict["ordered"] = ordered
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if candidate_threshold is not UNSET:
            field_dict["candidate_threshold"] = candidate_threshold
        if activation is not UNSET:
            field_dict["activation"] = activation
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if default is not UNSET:
            field_dict["default"] = default
        if prompt is not UNSET:
            field_dict["prompt"] = prompt
        if instruction is not UNSET:
            field_dict["instruction"] = instruction
        if examples is not UNSET:
            field_dict["examples"] = examples

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_classification_example_type_0 import ExtractionClassificationExampleType0
        from ..models.extraction_classification_schema_label_definitions import (
            ExtractionClassificationSchemaLabelDefinitions,
        )

        d = dict(src_dict)
        name = d.pop("name")

        labels = cast(list[str], d.pop("labels"))

        multi_label = d.pop("multi_label", UNSET)

        hypothesis_template = d.pop("hypothesis_template", UNSET)

        top_k = d.pop("top_k", UNSET)

        _mode = d.pop("mode", UNSET)
        mode: ExtractionClassificationSchemaMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = ExtractionClassificationSchemaMode(_mode)

        _label_definitions = d.pop("label_definitions", UNSET)
        label_definitions: ExtractionClassificationSchemaLabelDefinitions | Unset
        if isinstance(_label_definitions, Unset):
            label_definitions = UNSET
        else:
            label_definitions = ExtractionClassificationSchemaLabelDefinitions.from_dict(_label_definitions)

        min_labels = d.pop("min_labels", UNSET)

        def _parse_max_labels(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        max_labels = _parse_max_labels(d.pop("max_labels", UNSET))

        ordered = d.pop("ordered", UNSET)

        threshold = d.pop("threshold", UNSET)

        candidate_threshold = d.pop("candidate_threshold", UNSET)

        _activation = d.pop("activation", UNSET)
        activation: ExtractionClassificationSchemaActivation | Unset
        if isinstance(_activation, Unset):
            activation = UNSET
        else:
            activation = ExtractionClassificationSchemaActivation(_activation)

        temperature = d.pop("temperature", UNSET)

        default = d.pop("default", UNSET)

        prompt = d.pop("prompt", UNSET)

        instruction = d.pop("instruction", UNSET)

        _examples = d.pop("examples", UNSET)
        examples: list[ExtractionClassificationExampleType0 | list[str]] | Unset = UNSET
        if _examples is not UNSET:
            examples = []
            for examples_item_data in _examples:

                def _parse_examples_item(data: object) -> ExtractionClassificationExampleType0 | list[str]:
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_extraction_classification_example_type_0 = (
                            ExtractionClassificationExampleType0.from_dict(data)
                        )

                        return componentsschemas_extraction_classification_example_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, list):
                        raise TypeError()
                    componentsschemas_extraction_classification_example_type_1 = cast(list[str], data)

                    return componentsschemas_extraction_classification_example_type_1

                examples_item = _parse_examples_item(examples_item_data)

                examples.append(examples_item)

        extraction_classification_schema = cls(
            name=name,
            labels=labels,
            multi_label=multi_label,
            hypothesis_template=hypothesis_template,
            top_k=top_k,
            mode=mode,
            label_definitions=label_definitions,
            min_labels=min_labels,
            max_labels=max_labels,
            ordered=ordered,
            threshold=threshold,
            candidate_threshold=candidate_threshold,
            activation=activation,
            temperature=temperature,
            default=default,
            prompt=prompt,
            instruction=instruction,
            examples=examples,
        )

        extraction_classification_schema.additional_properties = d
        return extraction_classification_schema

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
