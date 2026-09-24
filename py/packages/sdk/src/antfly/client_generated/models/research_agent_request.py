from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.agent_decision import AgentDecision
    from ..models.chain_link import ChainLink
    from ..models.chat_message import ChatMessage
    from ..models.chat_tools_config import ChatToolsConfig
    from ..models.filter_spec import FilterSpec
    from ..models.generator_config import GeneratorConfig
    from ..models.query_request import QueryRequest
    from ..models.research_agent_steps import ResearchAgentSteps
    from ..models.research_budget import ResearchBudget
    from ..models.research_state import ResearchState


T = TypeVar("T", bound="ResearchAgentRequest")


@_attrs_define
class ResearchAgentRequest:
    """Request for the research agent. The agent plans sub-questions, runs a
    bounded retrieval researcher per sub-question in parallel, reflects on
    coverage, and writes a long-form report whose `[E#]` citations resolve
    to a deduplicated evidence registry.

    Researchers are ordinary retrieval-agent runs over `queries` with the
    same authorization, mandatory predicates and tool policy. They cannot
    widen tables, filters, tools or budgets.

        Attributes:
            query (str): The research question. Example: How do Antfly's hybrid search and reranking interact, and what are
                the tuning trade-offs?.
            queries (list[QueryRequest]): Authorized table scopes, as for the retrieval agent. `filter_query`
                and `exclusion_query` are mandatory predicates for every researcher.
                May be empty when web search is enabled.
            messages (list[ChatMessage] | Unset): Optional conversational context.
            agent_knowledge (str | Unset): Domain context for every role.
            accumulated_filters (list[FilterSpec] | Unset): Mandatory filters applied to every researcher search.
            session_id (str | Unset): Correlation identifier echoed back to the client.
            decisions (list[AgentDecision] | Unset): Structured user answers for client-carried continuation.
            interactive (bool | Unset): If true, the planner may return clarification questions instead of a plan. Default:
                False.
            generator (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
                 Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            chain (list[ChainLink] | Unset): Default chain of generators for every role.
            tools (ChatToolsConfig | Unset): Configuration for retrieval agent tools.

                If `enabled_tools` is empty/omitted, retrieval agents default to all retrieval tools
                available for the request. Explicit retrieval policies should use semantic_search
                for vector retrieval.

                For models that don't support native tool calling (e.g., Ollama),
                a prompt-based fallback is used with structured output parsing.
            steps (ResearchAgentSteps | Unset): Per-role configuration for the research agent.
            budget (ResearchBudget | Unset): Declared upper bounds for one research run. The worst-case LLM and
                tool-call cost is computable before execution; requests whose worst
                case exceeds the server ceiling are rejected, not clamped.
            research_state (ResearchState | Unset): Client-carried continuation state. Sending it back resumes the run at
                `phase` without repeating completed work. It never contains raw tool
                transcripts, credentials or connection settings. Evidence snippets are
                bounded excerpts of documents the caller was authorized to read; every
                resumed request is re-authorized.
            max_context_tokens (int | Unset): Per-researcher tool-result context budget in tokens.
            reserve_tokens (int | Unset): Tokens reserved from max_context_tokens for prompts and answers.
            stream (bool | Unset): Enable SSE streaming vs JSON response. Default: True.
    """

    query: str
    queries: list[QueryRequest]
    messages: list[ChatMessage] | Unset = UNSET
    agent_knowledge: str | Unset = UNSET
    accumulated_filters: list[FilterSpec] | Unset = UNSET
    session_id: str | Unset = UNSET
    decisions: list[AgentDecision] | Unset = UNSET
    interactive: bool | Unset = False
    generator: GeneratorConfig | Unset = UNSET
    chain: list[ChainLink] | Unset = UNSET
    tools: ChatToolsConfig | Unset = UNSET
    steps: ResearchAgentSteps | Unset = UNSET
    budget: ResearchBudget | Unset = UNSET
    research_state: ResearchState | Unset = UNSET
    max_context_tokens: int | Unset = UNSET
    reserve_tokens: int | Unset = UNSET
    stream: bool | Unset = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        query = self.query

        queries = []
        for queries_item_data in self.queries:
            queries_item = queries_item_data.to_dict()
            queries.append(queries_item)

        messages: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.messages, Unset):
            messages = []
            for messages_item_data in self.messages:
                messages_item = messages_item_data.to_dict()
                messages.append(messages_item)

        agent_knowledge = self.agent_knowledge

        accumulated_filters: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.accumulated_filters, Unset):
            accumulated_filters = []
            for accumulated_filters_item_data in self.accumulated_filters:
                accumulated_filters_item = accumulated_filters_item_data.to_dict()
                accumulated_filters.append(accumulated_filters_item)

        session_id = self.session_id

        decisions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.decisions, Unset):
            decisions = []
            for decisions_item_data in self.decisions:
                decisions_item = decisions_item_data.to_dict()
                decisions.append(decisions_item)

        interactive = self.interactive

        generator: dict[str, Any] | Unset = UNSET
        if not isinstance(self.generator, Unset):
            generator = self.generator.to_dict()

        chain: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.chain, Unset):
            chain = []
            for chain_item_data in self.chain:
                chain_item = chain_item_data.to_dict()
                chain.append(chain_item)

        tools: dict[str, Any] | Unset = UNSET
        if not isinstance(self.tools, Unset):
            tools = self.tools.to_dict()

        steps: dict[str, Any] | Unset = UNSET
        if not isinstance(self.steps, Unset):
            steps = self.steps.to_dict()

        budget: dict[str, Any] | Unset = UNSET
        if not isinstance(self.budget, Unset):
            budget = self.budget.to_dict()

        research_state: dict[str, Any] | Unset = UNSET
        if not isinstance(self.research_state, Unset):
            research_state = self.research_state.to_dict()

        max_context_tokens = self.max_context_tokens

        reserve_tokens = self.reserve_tokens

        stream = self.stream

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "query": query,
                "queries": queries,
            }
        )
        if messages is not UNSET:
            field_dict["messages"] = messages
        if agent_knowledge is not UNSET:
            field_dict["agent_knowledge"] = agent_knowledge
        if accumulated_filters is not UNSET:
            field_dict["accumulated_filters"] = accumulated_filters
        if session_id is not UNSET:
            field_dict["session_id"] = session_id
        if decisions is not UNSET:
            field_dict["decisions"] = decisions
        if interactive is not UNSET:
            field_dict["interactive"] = interactive
        if generator is not UNSET:
            field_dict["generator"] = generator
        if chain is not UNSET:
            field_dict["chain"] = chain
        if tools is not UNSET:
            field_dict["tools"] = tools
        if steps is not UNSET:
            field_dict["steps"] = steps
        if budget is not UNSET:
            field_dict["budget"] = budget
        if research_state is not UNSET:
            field_dict["research_state"] = research_state
        if max_context_tokens is not UNSET:
            field_dict["max_context_tokens"] = max_context_tokens
        if reserve_tokens is not UNSET:
            field_dict["reserve_tokens"] = reserve_tokens
        if stream is not UNSET:
            field_dict["stream"] = stream

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.agent_decision import AgentDecision
        from ..models.chain_link import ChainLink
        from ..models.chat_message import ChatMessage
        from ..models.chat_tools_config import ChatToolsConfig
        from ..models.filter_spec import FilterSpec
        from ..models.generator_config import GeneratorConfig
        from ..models.query_request import QueryRequest
        from ..models.research_agent_steps import ResearchAgentSteps
        from ..models.research_budget import ResearchBudget
        from ..models.research_state import ResearchState

        d = dict(src_dict)
        query = d.pop("query")

        queries = []
        _queries = d.pop("queries")
        for queries_item_data in _queries:
            queries_item = QueryRequest.from_dict(queries_item_data)

            queries.append(queries_item)

        _messages = d.pop("messages", UNSET)
        messages: list[ChatMessage] | Unset = UNSET
        if _messages is not UNSET:
            messages = []
            for messages_item_data in _messages:
                messages_item = ChatMessage.from_dict(messages_item_data)

                messages.append(messages_item)

        agent_knowledge = d.pop("agent_knowledge", UNSET)

        _accumulated_filters = d.pop("accumulated_filters", UNSET)
        accumulated_filters: list[FilterSpec] | Unset = UNSET
        if _accumulated_filters is not UNSET:
            accumulated_filters = []
            for accumulated_filters_item_data in _accumulated_filters:
                accumulated_filters_item = FilterSpec.from_dict(accumulated_filters_item_data)

                accumulated_filters.append(accumulated_filters_item)

        session_id = d.pop("session_id", UNSET)

        _decisions = d.pop("decisions", UNSET)
        decisions: list[AgentDecision] | Unset = UNSET
        if _decisions is not UNSET:
            decisions = []
            for decisions_item_data in _decisions:
                decisions_item = AgentDecision.from_dict(decisions_item_data)

                decisions.append(decisions_item)

        interactive = d.pop("interactive", UNSET)

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

        _tools = d.pop("tools", UNSET)
        tools: ChatToolsConfig | Unset
        if isinstance(_tools, Unset):
            tools = UNSET
        else:
            tools = ChatToolsConfig.from_dict(_tools)

        _steps = d.pop("steps", UNSET)
        steps: ResearchAgentSteps | Unset
        if isinstance(_steps, Unset):
            steps = UNSET
        else:
            steps = ResearchAgentSteps.from_dict(_steps)

        _budget = d.pop("budget", UNSET)
        budget: ResearchBudget | Unset
        if isinstance(_budget, Unset):
            budget = UNSET
        else:
            budget = ResearchBudget.from_dict(_budget)

        _research_state = d.pop("research_state", UNSET)
        research_state: ResearchState | Unset
        if isinstance(_research_state, Unset):
            research_state = UNSET
        else:
            research_state = ResearchState.from_dict(_research_state)

        max_context_tokens = d.pop("max_context_tokens", UNSET)

        reserve_tokens = d.pop("reserve_tokens", UNSET)

        stream = d.pop("stream", UNSET)

        research_agent_request = cls(
            query=query,
            queries=queries,
            messages=messages,
            agent_knowledge=agent_knowledge,
            accumulated_filters=accumulated_filters,
            session_id=session_id,
            decisions=decisions,
            interactive=interactive,
            generator=generator,
            chain=chain,
            tools=tools,
            steps=steps,
            budget=budget,
            research_state=research_state,
            max_context_tokens=max_context_tokens,
            reserve_tokens=reserve_tokens,
            stream=stream,
        )

        research_agent_request.additional_properties = d
        return research_agent_request

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
