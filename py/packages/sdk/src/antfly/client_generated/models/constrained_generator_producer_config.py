from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.constrained_output import ConstrainedOutput
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.constrained_generation_batch import ConstrainedGenerationBatch
    from ..models.constrained_generator_producer_config_output_schema import (
        ConstrainedGeneratorProducerConfigOutputSchema,
    )
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="ConstrainedGeneratorProducerConfig")


@_attrs_define
class ConstrainedGeneratorProducerConfig:
    """
    Attributes:
        generator (GeneratorConfig): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
        output_schema (ConstrainedGeneratorProducerConfigOutputSchema): JSON schema enforced before artifact
            publication. Unsupported schema vocabulary fails admission. Invalid or missing batch items receive one
            validation retry; successful siblings are retained.
        instructions (str | Unset):
        output (ConstrainedOutput | Unset): Provider and model admission selects a supported constrained mechanism. Auto
            prefers forced tools, then provider JSON schema, then native grammar. Free-text parsing is not a mode.
        batch (ConstrainedGenerationBatch | Unset):
    """

    generator: GeneratorConfig
    output_schema: ConstrainedGeneratorProducerConfigOutputSchema
    instructions: str | Unset = UNSET
    output: ConstrainedOutput | Unset = UNSET
    batch: ConstrainedGenerationBatch | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        generator = self.generator.to_dict()

        output_schema = self.output_schema.to_dict()

        instructions = self.instructions

        output: str | Unset = UNSET
        if not isinstance(self.output, Unset):
            output = self.output.value

        batch: dict[str, Any] | Unset = UNSET
        if not isinstance(self.batch, Unset):
            batch = self.batch.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "generator": generator,
                "output_schema": output_schema,
            }
        )
        if instructions is not UNSET:
            field_dict["instructions"] = instructions
        if output is not UNSET:
            field_dict["output"] = output
        if batch is not UNSET:
            field_dict["batch"] = batch

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.constrained_generation_batch import ConstrainedGenerationBatch
        from ..models.constrained_generator_producer_config_output_schema import (
            ConstrainedGeneratorProducerConfigOutputSchema,
        )
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        generator = GeneratorConfig.from_dict(d.pop("generator"))

        output_schema = ConstrainedGeneratorProducerConfigOutputSchema.from_dict(d.pop("output_schema"))

        instructions = d.pop("instructions", UNSET)

        _output = d.pop("output", UNSET)
        output: ConstrainedOutput | Unset
        if isinstance(_output, Unset):
            output = UNSET
        else:
            output = ConstrainedOutput(_output)

        _batch = d.pop("batch", UNSET)
        batch: ConstrainedGenerationBatch | Unset
        if isinstance(_batch, Unset):
            batch = UNSET
        else:
            batch = ConstrainedGenerationBatch.from_dict(_batch)

        constrained_generator_producer_config = cls(
            generator=generator,
            output_schema=output_schema,
            instructions=instructions,
            output=output,
            batch=batch,
        )

        return constrained_generator_producer_config
