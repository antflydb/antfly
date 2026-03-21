from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chain_link import ChainLink
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="GenerationStepConfig")


@_attrs_define
class GenerationStepConfig:
    """Configuration for the generation step. This step generates the final
    response from retrieved documents using the reasoning as context.

        Attributes:
            enabled (Union[Unset, bool]): Enable generation from retrieved documents Default: False.
            generator (Union[Unset, GeneratorConfig]): A unified configuration for a generative AI provider.
                 Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            chain (Union[Unset, list['ChainLink']]): Chain of generators to try in order. Mutually exclusive with
                'generator'.
            system_prompt (Union[Unset, str]): Custom system prompt for answer generation
            generation_context (Union[Unset, str]): Custom guidance for generation tone, detail level, and style Example: Be
                concise and technical. Include code examples where relevant..
    """

    enabled: Union[Unset, bool] = False
    generator: Union[Unset, "GeneratorConfig"] = UNSET
    chain: Union[Unset, list["ChainLink"]] = UNSET
    system_prompt: Union[Unset, str] = UNSET
    generation_context: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        generator: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.generator, Unset):
            generator = self.generator.to_dict()

        chain: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.chain, Unset):
            chain = []
            for chain_item_data in self.chain:
                chain_item = chain_item_data.to_dict()
                chain.append(chain_item)

        system_prompt = self.system_prompt

        generation_context = self.generation_context

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if enabled is not UNSET:
            field_dict["enabled"] = enabled
        if generator is not UNSET:
            field_dict["generator"] = generator
        if chain is not UNSET:
            field_dict["chain"] = chain
        if system_prompt is not UNSET:
            field_dict["system_prompt"] = system_prompt
        if generation_context is not UNSET:
            field_dict["generation_context"] = generation_context

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chain_link import ChainLink
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        enabled = d.pop("enabled", UNSET)

        _generator = d.pop("generator", UNSET)
        generator: Union[Unset, GeneratorConfig]
        if isinstance(_generator, Unset):
            generator = UNSET
        else:
            generator = GeneratorConfig.from_dict(_generator)

        chain = []
        _chain = d.pop("chain", UNSET)
        for chain_item_data in _chain or []:
            chain_item = ChainLink.from_dict(chain_item_data)

            chain.append(chain_item)

        system_prompt = d.pop("system_prompt", UNSET)

        generation_context = d.pop("generation_context", UNSET)

        generation_step_config = cls(
            enabled=enabled,
            generator=generator,
            chain=chain,
            system_prompt=system_prompt,
            generation_context=generation_context,
        )

        generation_step_config.additional_properties = d
        return generation_step_config

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
