from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chain_link import ChainLink
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="ResearchWriteStepConfig")


@_attrs_define
class ResearchWriteStepConfig:
    """Configuration for the report writer.

    Attributes:
        generator (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
        chain (list[ChainLink] | Unset): Chain of generators for the writer.
        instructions (str | Unset): Additional writer instructions, for example audience or tone.
        outline (list[str] | Unset): Optional caller-supplied section headings. When omitted the writer chooses them.
    """

    generator: GeneratorConfig | Unset = UNSET
    chain: list[ChainLink] | Unset = UNSET
    instructions: str | Unset = UNSET
    outline: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        generator: dict[str, Any] | Unset = UNSET
        if not isinstance(self.generator, Unset):
            generator = self.generator.to_dict()

        chain: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.chain, Unset):
            chain = []
            for chain_item_data in self.chain:
                chain_item = chain_item_data.to_dict()
                chain.append(chain_item)

        instructions = self.instructions

        outline: list[str] | Unset = UNSET
        if not isinstance(self.outline, Unset):
            outline = self.outline

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if generator is not UNSET:
            field_dict["generator"] = generator
        if chain is not UNSET:
            field_dict["chain"] = chain
        if instructions is not UNSET:
            field_dict["instructions"] = instructions
        if outline is not UNSET:
            field_dict["outline"] = outline

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chain_link import ChainLink
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        _generator = d.pop("generator", UNSET)
        generator: GeneratorConfig | Unset
        if isinstance(_generator, Unset):
            generator = UNSET
        else:
            generator = GeneratorConfig.from_dict(_generator)

        _chain = d.pop("chain", UNSET)
        chain: list[ChainLink] | Unset = UNSET
        if _chain is not UNSET:
            chain = []
            for chain_item_data in _chain:
                chain_item = ChainLink.from_dict(chain_item_data)

                chain.append(chain_item)

        instructions = d.pop("instructions", UNSET)

        outline = cast(list[str], d.pop("outline", UNSET))

        research_write_step_config = cls(
            generator=generator,
            chain=chain,
            instructions=instructions,
            outline=outline,
        )

        research_write_step_config.additional_properties = d
        return research_write_step_config

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
