from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.constrained_output import ConstrainedOutput
from ..models.constrained_probabilities import ConstrainedProbabilities
from ..models.generated_extractor_config_method import GeneratedExtractorConfigMethod
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.constrained_generation_batch import ConstrainedGenerationBatch
    from ..models.extraction_options import ExtractionOptions
    from ..models.extraction_schema import ExtractionSchema
    from ..models.generated_extractor_config_entity_definitions import GeneratedExtractorConfigEntityDefinitions
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="GeneratedExtractorConfig")


@_attrs_define
class GeneratedExtractorConfig:
    """
    Attributes:
        method (GeneratedExtractorConfigMethod):
        generator (GeneratorConfig): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
        schema (ExtractionSchema): Version 1 selects one extraction family; entities may accompany relations.
            With schema_version 2, entities, attributes, classifications, structures,
            and ordinary relations may share one encoded input. joint_ie is a separate,
            mutually exclusive typed graph schema. The version 2 compiler rejects
            unknown fields and validates all references before model execution.
        entity_definitions (GeneratedExtractorConfigEntityDefinitions | Unset):
        instructions (str | Unset):
        output (ConstrainedOutput | Unset): Provider and model admission selects a supported constrained mechanism. Auto
            prefers forced tools, then provider JSON schema, then native grammar. Free-text parsing is not a mode.
        probabilities (ConstrainedProbabilities | Unset): Token probabilities require complete decoder evidence for the
            allowed values. Verdict-only is explicit opt-in; model-reported confidence is never used.
        batch (ConstrainedGenerationBatch | Unset):
        options (ExtractionOptions | Unset):
    """

    method: GeneratedExtractorConfigMethod
    generator: GeneratorConfig
    schema: ExtractionSchema
    entity_definitions: GeneratedExtractorConfigEntityDefinitions | Unset = UNSET
    instructions: str | Unset = UNSET
    output: ConstrainedOutput | Unset = UNSET
    probabilities: ConstrainedProbabilities | Unset = UNSET
    batch: ConstrainedGenerationBatch | Unset = UNSET
    options: ExtractionOptions | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        method = self.method.value

        generator = self.generator.to_dict()

        schema = self.schema.to_dict()

        entity_definitions: dict[str, Any] | Unset = UNSET
        if not isinstance(self.entity_definitions, Unset):
            entity_definitions = self.entity_definitions.to_dict()

        instructions = self.instructions

        output: str | Unset = UNSET
        if not isinstance(self.output, Unset):
            output = self.output.value

        probabilities: str | Unset = UNSET
        if not isinstance(self.probabilities, Unset):
            probabilities = self.probabilities.value

        batch: dict[str, Any] | Unset = UNSET
        if not isinstance(self.batch, Unset):
            batch = self.batch.to_dict()

        options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.options, Unset):
            options = self.options.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "method": method,
                "generator": generator,
                "schema": schema,
            }
        )
        if entity_definitions is not UNSET:
            field_dict["entity_definitions"] = entity_definitions
        if instructions is not UNSET:
            field_dict["instructions"] = instructions
        if output is not UNSET:
            field_dict["output"] = output
        if probabilities is not UNSET:
            field_dict["probabilities"] = probabilities
        if batch is not UNSET:
            field_dict["batch"] = batch
        if options is not UNSET:
            field_dict["options"] = options

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.constrained_generation_batch import ConstrainedGenerationBatch
        from ..models.extraction_options import ExtractionOptions
        from ..models.extraction_schema import ExtractionSchema
        from ..models.generated_extractor_config_entity_definitions import GeneratedExtractorConfigEntityDefinitions
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        method = GeneratedExtractorConfigMethod(d.pop("method"))

        generator = GeneratorConfig.from_dict(d.pop("generator"))

        schema = ExtractionSchema.from_dict(d.pop("schema"))

        _entity_definitions = d.pop("entity_definitions", UNSET)
        entity_definitions: GeneratedExtractorConfigEntityDefinitions | Unset
        if isinstance(_entity_definitions, Unset):
            entity_definitions = UNSET
        else:
            entity_definitions = GeneratedExtractorConfigEntityDefinitions.from_dict(_entity_definitions)

        instructions = d.pop("instructions", UNSET)

        _output = d.pop("output", UNSET)
        output: ConstrainedOutput | Unset
        if isinstance(_output, Unset):
            output = UNSET
        else:
            output = ConstrainedOutput(_output)

        _probabilities = d.pop("probabilities", UNSET)
        probabilities: ConstrainedProbabilities | Unset
        if isinstance(_probabilities, Unset):
            probabilities = UNSET
        else:
            probabilities = ConstrainedProbabilities(_probabilities)

        _batch = d.pop("batch", UNSET)
        batch: ConstrainedGenerationBatch | Unset
        if isinstance(_batch, Unset):
            batch = UNSET
        else:
            batch = ConstrainedGenerationBatch.from_dict(_batch)

        _options = d.pop("options", UNSET)
        options: ExtractionOptions | Unset
        if isinstance(_options, Unset):
            options = UNSET
        else:
            options = ExtractionOptions.from_dict(_options)

        generated_extractor_config = cls(
            method=method,
            generator=generator,
            schema=schema,
            entity_definitions=entity_definitions,
            instructions=instructions,
            output=output,
            probabilities=probabilities,
            batch=batch,
            options=options,
        )

        return generated_extractor_config
