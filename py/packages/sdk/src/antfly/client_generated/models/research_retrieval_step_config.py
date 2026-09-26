from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chain_link import ChainLink
    from ..models.chat_tools_config import ChatToolsConfig
    from ..models.generator_config import GeneratorConfig
    from ..models.retrieval_navigation_config import RetrievalNavigationConfig


T = TypeVar("T", bound="ResearchRetrievalStepConfig")


@_attrs_define
class ResearchRetrievalStepConfig:
    """Configuration for researchers. Every researcher is a bounded retrieval
    agent run over the request's authorized queries. `tools` narrows the
    top-level tools policy and cannot widen it.

        Attributes:
            generator (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
                 Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            chain (list[ChainLink] | Unset): Chain of generators for researchers.
            instructions (str | Unset): Additional researcher instructions.
            tools (ChatToolsConfig | Unset): Configuration for retrieval agent tools.

                If `enabled_tools` is empty/omitted, retrieval agents default to all retrieval tools
                available for the request. Explicit retrieval policies should use semantic_search
                for vector retrieval.

                For models that don't support native tool calling (e.g., Ollama),
                a prompt-based fallback is used with structured output parsing.
            navigation (RetrievalNavigationConfig | Unset): Retrieval-step navigation targeting one ordinary query. Graph
                navigation
                follows one path; tree navigation explores a retained branch frontier.
                Agentic selection uses the enclosing model and budgets. Ranked selection
                is supported for trees and uses the existing deterministic tree traversal.
                Agentic selection requires agentic mode and a retrieval generator. Search
                starts exploration; navigation selects only an offered, unvisited node.
                All reads enforce mandatory predicates and authenticated row filters.
    """

    generator: GeneratorConfig | Unset = UNSET
    chain: list[ChainLink] | Unset = UNSET
    instructions: str | Unset = UNSET
    tools: ChatToolsConfig | Unset = UNSET
    navigation: RetrievalNavigationConfig | Unset = UNSET
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

        tools: dict[str, Any] | Unset = UNSET
        if not isinstance(self.tools, Unset):
            tools = self.tools.to_dict()

        navigation: dict[str, Any] | Unset = UNSET
        if not isinstance(self.navigation, Unset):
            navigation = self.navigation.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if generator is not UNSET:
            field_dict["generator"] = generator
        if chain is not UNSET:
            field_dict["chain"] = chain
        if instructions is not UNSET:
            field_dict["instructions"] = instructions
        if tools is not UNSET:
            field_dict["tools"] = tools
        if navigation is not UNSET:
            field_dict["navigation"] = navigation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chain_link import ChainLink
        from ..models.chat_tools_config import ChatToolsConfig
        from ..models.generator_config import GeneratorConfig
        from ..models.retrieval_navigation_config import RetrievalNavigationConfig

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

        _tools = d.pop("tools", UNSET)
        tools: ChatToolsConfig | Unset
        if isinstance(_tools, Unset):
            tools = UNSET
        else:
            tools = ChatToolsConfig.from_dict(_tools)

        _navigation = d.pop("navigation", UNSET)
        navigation: RetrievalNavigationConfig | Unset
        if isinstance(_navigation, Unset):
            navigation = UNSET
        else:
            navigation = RetrievalNavigationConfig.from_dict(_navigation)

        research_retrieval_step_config = cls(
            generator=generator,
            chain=chain,
            instructions=instructions,
            tools=tools,
            navigation=navigation,
        )

        research_retrieval_step_config.additional_properties = d
        return research_retrieval_step_config

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
