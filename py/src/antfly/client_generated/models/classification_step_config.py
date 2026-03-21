from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_strategy import QueryStrategy
from ..models.semantic_query_mode import SemanticQueryMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chain_link import ChainLink
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="ClassificationStepConfig")


@_attrs_define
class ClassificationStepConfig:
    """Configuration for the classification step. This step analyzes the query,
    selects the optimal retrieval strategy, and generates semantic transformations.

        Attributes:
            enabled (Union[Unset, bool]): Enable query classification and strategy selection Default: False.
            generator (Union[Unset, GeneratorConfig]): A unified configuration for a generative AI provider.
                 Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            chain (Union[Unset, list['ChainLink']]): Chain of generators to try in order. Mutually exclusive with
                'generator'.
            with_reasoning (Union[Unset, bool]): Include pre-retrieval reasoning explaining query analysis and strategy
                selection Default: False.
            force_strategy (Union[Unset, QueryStrategy]): Strategy for query transformation and retrieval:
                - simple: Direct query with multi-phrase expansion. Best for straightforward factual queries.
                - decompose: Break complex queries into sub-questions, retrieve for each. Best for multi-part questions.
                - step_back: Generate broader background query first, then specific query. Best for questions needing context.
                - hyde: Generate hypothetical answer document, embed that for retrieval. Best for abstract/conceptual questions.
            force_semantic_mode (Union[Unset, SemanticQueryMode]): Mode for semantic query generation:
                - rewrite: Transform query into expanded keywords/concepts optimized for vector search (Level 2 optimization)
                - hypothetical: Generate a hypothetical answer that would appear in relevant documents (HyDE - Level 3
                optimization)
            multi_phrase_count (Union[Unset, int]): Number of alternative query phrasings to generate Default: 3.
    """

    enabled: Union[Unset, bool] = False
    generator: Union[Unset, "GeneratorConfig"] = UNSET
    chain: Union[Unset, list["ChainLink"]] = UNSET
    with_reasoning: Union[Unset, bool] = False
    force_strategy: Union[Unset, QueryStrategy] = UNSET
    force_semantic_mode: Union[Unset, SemanticQueryMode] = UNSET
    multi_phrase_count: Union[Unset, int] = 3
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

        with_reasoning = self.with_reasoning

        force_strategy: Union[Unset, str] = UNSET
        if not isinstance(self.force_strategy, Unset):
            force_strategy = self.force_strategy.value

        force_semantic_mode: Union[Unset, str] = UNSET
        if not isinstance(self.force_semantic_mode, Unset):
            force_semantic_mode = self.force_semantic_mode.value

        multi_phrase_count = self.multi_phrase_count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if enabled is not UNSET:
            field_dict["enabled"] = enabled
        if generator is not UNSET:
            field_dict["generator"] = generator
        if chain is not UNSET:
            field_dict["chain"] = chain
        if with_reasoning is not UNSET:
            field_dict["with_reasoning"] = with_reasoning
        if force_strategy is not UNSET:
            field_dict["force_strategy"] = force_strategy
        if force_semantic_mode is not UNSET:
            field_dict["force_semantic_mode"] = force_semantic_mode
        if multi_phrase_count is not UNSET:
            field_dict["multi_phrase_count"] = multi_phrase_count

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

        with_reasoning = d.pop("with_reasoning", UNSET)

        _force_strategy = d.pop("force_strategy", UNSET)
        force_strategy: Union[Unset, QueryStrategy]
        if isinstance(_force_strategy, Unset):
            force_strategy = UNSET
        else:
            force_strategy = QueryStrategy(_force_strategy)

        _force_semantic_mode = d.pop("force_semantic_mode", UNSET)
        force_semantic_mode: Union[Unset, SemanticQueryMode]
        if isinstance(_force_semantic_mode, Unset):
            force_semantic_mode = UNSET
        else:
            force_semantic_mode = SemanticQueryMode(_force_semantic_mode)

        multi_phrase_count = d.pop("multi_phrase_count", UNSET)

        classification_step_config = cls(
            enabled=enabled,
            generator=generator,
            chain=chain,
            with_reasoning=with_reasoning,
            force_strategy=force_strategy,
            force_semantic_mode=force_semantic_mode,
            multi_phrase_count=multi_phrase_count,
        )

        classification_step_config.additional_properties = d
        return classification_step_config

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
