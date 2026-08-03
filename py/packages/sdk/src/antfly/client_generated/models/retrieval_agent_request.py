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
    from ..models.retrieval_agent_steps import RetrievalAgentSteps
    from ..models.retrieval_query_request import RetrievalQueryRequest


T = TypeVar("T", bound="RetrievalAgentRequest")


@_attrs_define
class RetrievalAgentRequest:
    """Request for the retrieval agent. Queries define which tables and indexes
    to search, each as a QueryRequest with optional tree search configuration.

    **Pipeline mode** (default, max_internal_iterations=0): Queries are executed
    directly without an LLM tool-calling loop.

    **Agentic mode** (max_internal_iterations > 0): The LLM decides which tools to
    call, using the queries to determine available tables and indexes.

        Attributes:
            query (str): User's natural language query Example: How do I configure OAuth?.
            queries (list[RetrievalQueryRequest]): Queries to execute. Each query carries its own table via the
                QueryRequest table field.

                In pipeline mode (max_internal_iterations=0), these are executed directly.
                In agentic mode, these declare which table and indexes are available.
                 Example: [{'table': 'docs', 'semantic_search': 'How do I configure OAuth?', 'indexes': ['doc_embeddings'],
                'limit': 10}].
            messages (list[ChatMessage] | Unset): Optional conversational context for the current turn. Decisions remain the
                authoritative continuation input for bounded agent interactions.
            agent_knowledge (str | Unset): Domain-specific knowledge to include in the agent's system prompt.
                Useful for providing context about the document collection.
                 Example: This collection contains API documentation for the Acme product suite..
            accumulated_filters (list[FilterSpec] | Unset): Pre-applied filters from prior interactions. These are applied
                to
                all search tool invocations.
            session_id (str | Unset): Correlation identifier for a bounded agent interaction. In Phase 1 this is echoed back
                to the client but does not imply server-side session persistence.
            decisions (list[AgentDecision] | Unset): Structured answers provided by the user as part of client-carried
                continuation.
            interactive (bool | Unset): If true, the agent may return clarification questions when needed. Default: True.
            max_internal_iterations (int | Unset): Maximum number of internal tool-calling rounds.

                - 0: Pipeline mode — execute provided queries directly, no LLM loop
                - 1+: Agentic mode — LLM decides which tools to call
                 Default: 0.
            max_user_clarifications (int | Unset): Maximum number of clarification turns the agent may request from the
                user.
            require_decision_after (int | Unset): Force a user-facing decision after this many unresolved internal passes.
            max_context_tokens (int | Unset): Maximum tokens for document context in tool responses. Documents
                exceeding this limit are pruned to fit.
            reserve_tokens (int | Unset): Tokens to reserve for system prompt, answer generation, and other overhead.
                Subtracted from max_context_tokens to determine available context budget.
                Defaults to 4000 if max_context_tokens is set.
                 Default: 4000. Example: 4000.
            stream (bool | Unset): Enable SSE streaming vs JSON response Default: True.
            generator (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
                 Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            chain (list[ChainLink] | Unset): Chain of generators
            tools (ChatToolsConfig | Unset): Configuration for retrieval agent tools.

                If `enabled_tools` is empty/omitted, retrieval agents default to all retrieval tools
                available for the request. Explicit retrieval policies should use semantic_search
                for vector retrieval.

                For models that don't support native tool calling (e.g., Ollama),
                a prompt-based fallback is used with structured output parsing.
            steps (RetrievalAgentSteps | Unset): Configuration for the retrieval agent's pipeline steps and tool-use
                behavior.
                Each step can have its own generator (or chain of generators) and step-specific options.
                If a step is not configured, it is skipped (retrieval always runs).
            document_renderer (str | Unset): Handlebars template for rendering documents in the generation prompt.
                Default uses TOON format for token efficiency.
                Requires steps.generation to be set.
                 Example: {{encodeToon this.fields}}.
    """

    query: str
    queries: list[RetrievalQueryRequest]
    messages: list[ChatMessage] | Unset = UNSET
    agent_knowledge: str | Unset = UNSET
    accumulated_filters: list[FilterSpec] | Unset = UNSET
    session_id: str | Unset = UNSET
    decisions: list[AgentDecision] | Unset = UNSET
    interactive: bool | Unset = True
    max_internal_iterations: int | Unset = 0
    max_user_clarifications: int | Unset = UNSET
    require_decision_after: int | Unset = UNSET
    max_context_tokens: int | Unset = UNSET
    reserve_tokens: int | Unset = 4000
    stream: bool | Unset = True
    generator: GeneratorConfig | Unset = UNSET
    chain: list[ChainLink] | Unset = UNSET
    tools: ChatToolsConfig | Unset = UNSET
    steps: RetrievalAgentSteps | Unset = UNSET
    document_renderer: str | Unset = UNSET
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

        max_internal_iterations = self.max_internal_iterations

        max_user_clarifications = self.max_user_clarifications

        require_decision_after = self.require_decision_after

        max_context_tokens = self.max_context_tokens

        reserve_tokens = self.reserve_tokens

        stream = self.stream

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

        document_renderer = self.document_renderer

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
        if max_internal_iterations is not UNSET:
            field_dict["max_internal_iterations"] = max_internal_iterations
        if max_user_clarifications is not UNSET:
            field_dict["max_user_clarifications"] = max_user_clarifications
        if require_decision_after is not UNSET:
            field_dict["require_decision_after"] = require_decision_after
        if max_context_tokens is not UNSET:
            field_dict["max_context_tokens"] = max_context_tokens
        if reserve_tokens is not UNSET:
            field_dict["reserve_tokens"] = reserve_tokens
        if stream is not UNSET:
            field_dict["stream"] = stream
        if generator is not UNSET:
            field_dict["generator"] = generator
        if chain is not UNSET:
            field_dict["chain"] = chain
        if tools is not UNSET:
            field_dict["tools"] = tools
        if steps is not UNSET:
            field_dict["steps"] = steps
        if document_renderer is not UNSET:
            field_dict["document_renderer"] = document_renderer

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.agent_decision import AgentDecision
        from ..models.chain_link import ChainLink
        from ..models.chat_message import ChatMessage
        from ..models.chat_tools_config import ChatToolsConfig
        from ..models.filter_spec import FilterSpec
        from ..models.generator_config import GeneratorConfig
        from ..models.retrieval_agent_steps import RetrievalAgentSteps
        from ..models.retrieval_query_request import RetrievalQueryRequest

        d = dict(src_dict)
        query = d.pop("query")

        queries = []
        _queries = d.pop("queries")
        for queries_item_data in _queries:
            queries_item = RetrievalQueryRequest.from_dict(queries_item_data)

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

        max_internal_iterations = d.pop("max_internal_iterations", UNSET)

        max_user_clarifications = d.pop("max_user_clarifications", UNSET)

        require_decision_after = d.pop("require_decision_after", UNSET)

        max_context_tokens = d.pop("max_context_tokens", UNSET)

        reserve_tokens = d.pop("reserve_tokens", UNSET)

        stream = d.pop("stream", UNSET)

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
        steps: RetrievalAgentSteps | Unset
        if isinstance(_steps, Unset):
            steps = UNSET
        else:
            steps = RetrievalAgentSteps.from_dict(_steps)

        document_renderer = d.pop("document_renderer", UNSET)

        retrieval_agent_request = cls(
            query=query,
            queries=queries,
            messages=messages,
            agent_knowledge=agent_knowledge,
            accumulated_filters=accumulated_filters,
            session_id=session_id,
            decisions=decisions,
            interactive=interactive,
            max_internal_iterations=max_internal_iterations,
            max_user_clarifications=max_user_clarifications,
            require_decision_after=require_decision_after,
            max_context_tokens=max_context_tokens,
            reserve_tokens=reserve_tokens,
            stream=stream,
            generator=generator,
            chain=chain,
            tools=tools,
            steps=steps,
            document_renderer=document_renderer,
        )

        retrieval_agent_request.additional_properties = d
        return retrieval_agent_request

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
