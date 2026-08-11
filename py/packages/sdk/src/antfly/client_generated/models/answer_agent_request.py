from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.answer_agent_steps import AnswerAgentSteps
    from ..models.chain_link import ChainLink
    from ..models.eval_config import EvalConfig
    from ..models.generator_config import GeneratorConfig
    from ..models.query_request import QueryRequest


T = TypeVar("T", bound="AnswerAgentRequest")


@_attrs_define
class AnswerAgentRequest:
    """DEPRECATED: Use RetrievalAgentRequest instead.
    Request for the answer agent. Accepts the old request format and
    internally delegates to the retrieval agent.

        Attributes:
            query (str): User's natural language query
            queries (list[QueryRequest]): Queries to execute. Each query specifies its own table.
            with_streaming (bool | Unset): DEPRECATED: Use stream on RetrievalAgentRequest instead.
                Enable SSE streaming vs JSON response.
                 Default: True.
            generator (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
                 Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            chain (list[ChainLink] | Unset): Chain of generators
            agent_knowledge (str | Unset): Domain-specific knowledge for the agent
            max_context_tokens (int | Unset): Maximum tokens for document context
            reserve_tokens (int | Unset): Tokens to reserve for overhead Default: 4000.
            steps (AnswerAgentSteps | Unset): DEPRECATED: Use RetrievalAgentSteps instead.
                Configuration for the answer agent's pipeline steps.
            eval_ (EvalConfig | Unset): Configuration for inline evaluation of query results.
                Add to RetrievalAgentRequest, QueryRequest, or other evaluation-capable request schemas.
            without_generation (bool | Unset): DEPRECATED: Omit steps.generation on RetrievalAgentRequest instead.
                If true, skip the generation step.
                 Default: False.
    """

    query: str
    queries: list[QueryRequest]
    with_streaming: bool | Unset = True
    generator: GeneratorConfig | Unset = UNSET
    chain: list[ChainLink] | Unset = UNSET
    agent_knowledge: str | Unset = UNSET
    max_context_tokens: int | Unset = UNSET
    reserve_tokens: int | Unset = 4000
    steps: AnswerAgentSteps | Unset = UNSET
    eval_: EvalConfig | Unset = UNSET
    without_generation: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        query = self.query

        queries = []
        for queries_item_data in self.queries:
            queries_item = queries_item_data.to_dict()
            queries.append(queries_item)

        with_streaming = self.with_streaming

        generator: dict[str, Any] | Unset = UNSET
        if not isinstance(self.generator, Unset):
            generator = self.generator.to_dict()

        chain: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.chain, Unset):
            chain = []
            for chain_item_data in self.chain:
                chain_item = chain_item_data.to_dict()
                chain.append(chain_item)

        agent_knowledge = self.agent_knowledge

        max_context_tokens = self.max_context_tokens

        reserve_tokens = self.reserve_tokens

        steps: dict[str, Any] | Unset = UNSET
        if not isinstance(self.steps, Unset):
            steps = self.steps.to_dict()

        eval_: dict[str, Any] | Unset = UNSET
        if not isinstance(self.eval_, Unset):
            eval_ = self.eval_.to_dict()

        without_generation = self.without_generation

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "query": query,
                "queries": queries,
            }
        )
        if with_streaming is not UNSET:
            field_dict["with_streaming"] = with_streaming
        if generator is not UNSET:
            field_dict["generator"] = generator
        if chain is not UNSET:
            field_dict["chain"] = chain
        if agent_knowledge is not UNSET:
            field_dict["agent_knowledge"] = agent_knowledge
        if max_context_tokens is not UNSET:
            field_dict["max_context_tokens"] = max_context_tokens
        if reserve_tokens is not UNSET:
            field_dict["reserve_tokens"] = reserve_tokens
        if steps is not UNSET:
            field_dict["steps"] = steps
        if eval_ is not UNSET:
            field_dict["eval"] = eval_
        if without_generation is not UNSET:
            field_dict["without_generation"] = without_generation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.answer_agent_steps import AnswerAgentSteps
        from ..models.chain_link import ChainLink
        from ..models.eval_config import EvalConfig
        from ..models.generator_config import GeneratorConfig
        from ..models.query_request import QueryRequest

        d = dict(src_dict)
        query = d.pop("query")

        queries = []
        _queries = d.pop("queries")
        for queries_item_data in _queries:
            queries_item = QueryRequest.from_dict(queries_item_data)

            queries.append(queries_item)

        with_streaming = d.pop("with_streaming", UNSET)

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

        agent_knowledge = d.pop("agent_knowledge", UNSET)

        max_context_tokens = d.pop("max_context_tokens", UNSET)

        reserve_tokens = d.pop("reserve_tokens", UNSET)

        _steps = d.pop("steps", UNSET)
        steps: AnswerAgentSteps | Unset
        if isinstance(_steps, Unset):
            steps = UNSET
        else:
            steps = AnswerAgentSteps.from_dict(_steps)

        _eval_ = d.pop("eval", UNSET)
        eval_: EvalConfig | Unset
        if isinstance(_eval_, Unset):
            eval_ = UNSET
        else:
            eval_ = EvalConfig.from_dict(_eval_)

        without_generation = d.pop("without_generation", UNSET)

        answer_agent_request = cls(
            query=query,
            queries=queries,
            with_streaming=with_streaming,
            generator=generator,
            chain=chain,
            agent_knowledge=agent_knowledge,
            max_context_tokens=max_context_tokens,
            reserve_tokens=reserve_tokens,
            steps=steps,
            eval_=eval_,
            without_generation=without_generation,
        )

        answer_agent_request.additional_properties = d
        return answer_agent_request

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
