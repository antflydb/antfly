from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.agent_status import AgentStatus
from ..models.retrieval_strategy import RetrievalStrategy
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.agent_question import AgentQuestion
    from ..models.agent_step import AgentStep
    from ..models.chat_message import ChatMessage
    from ..models.classification_transformation_result import ClassificationTransformationResult
    from ..models.eval_result import EvalResult
    from ..models.filter_spec import FilterSpec
    from ..models.incomplete_details import IncompleteDetails
    from ..models.query_hit import QueryHit
    from ..models.retrieval_agent_usage import RetrievalAgentUsage


T = TypeVar("T", bound="RetrievalAgentResult")


@_attrs_define
class RetrievalAgentResult:
    """Result from the retrieval agent

    Attributes:
        status (AgentStatus): Shared bounded-agent execution status
        hits (list[QueryHit]): Retrieved query hits
        id (str | Unset): Unique response ID for logging and tracing Example: ragr_cr3ig20h5tbs73e3ahrg.
        model (str | Unset): LLM model used for generation Example: gemini-2.0-flash.
        created_at (int | Unset): Unix timestamp (seconds) when the response was created
        incomplete_details (IncompleteDetails | Unset): Explains why the agent stopped before completion. Present when
            status is "incomplete".
        usage (RetrievalAgentUsage | Unset): Token usage and resource statistics from the retrieval agent execution
        steps (list[AgentStep] | Unset): Shared bounded-agent execution trace for this retrieval run.
        strategy_used (RetrievalStrategy | Unset): Strategy for document retrieval:
            - semantic: Vector similarity search using embeddings
            - bm25: Full-text search using BM25 scoring
            - metadata: Structured query on document fields
            - tree: Iterative tree navigation with summarization
            - graph: Relationship-based traversal
            - hybrid: Combine multiple strategies with RRF or rerank
        session_id (str | Unset): Correlation identifier for client-carried continuation.
        iteration (int | Unset): Current internal iteration count for this bounded session.
        clarification_count (int | Unset): Number of user clarification turns already consumed in this session.
        remaining_internal_iterations (int | Unset): Remaining internal reasoning/tool-use iterations for this session.
        remaining_user_clarifications (int | Unset): Remaining clarification turns allowed for this session.
        questions (list[AgentQuestion] | Unset): Clarification questions exposed in the shared bounded-agent envelope.
        applied_filters (list[FilterSpec] | Unset): Filters that were applied during retrieval
        tool_calls_made (int | Unset): Total number of tool calls made during retrieval
        messages (list[ChatMessage] | Unset): Optional conversational context including tool calls and responses.
            Decisions remain the authoritative continuation input for bounded agent interactions.
        classification (ClassificationTransformationResult | Unset): Query classification and transformation result
            combining all query enhancements including strategy selection and semantic optimization
        generation (str | Unset): Generated response in markdown format. Present when steps.generation
            was configured.
        generation_confidence (float | Unset): Confidence in the generated response (requires steps.confidence)
        context_relevance (float | Unset): Relevance of retrieved documents to the query (requires steps.confidence)
        followup_questions (list[str] | Unset): Suggested follow-up questions (requires steps.followup)
        eval_result (EvalResult | Unset): Complete evaluation result
    """

    status: AgentStatus
    hits: list[QueryHit]
    id: str | Unset = UNSET
    model: str | Unset = UNSET
    created_at: int | Unset = UNSET
    incomplete_details: IncompleteDetails | Unset = UNSET
    usage: RetrievalAgentUsage | Unset = UNSET
    steps: list[AgentStep] | Unset = UNSET
    strategy_used: RetrievalStrategy | Unset = UNSET
    session_id: str | Unset = UNSET
    iteration: int | Unset = UNSET
    clarification_count: int | Unset = UNSET
    remaining_internal_iterations: int | Unset = UNSET
    remaining_user_clarifications: int | Unset = UNSET
    questions: list[AgentQuestion] | Unset = UNSET
    applied_filters: list[FilterSpec] | Unset = UNSET
    tool_calls_made: int | Unset = UNSET
    messages: list[ChatMessage] | Unset = UNSET
    classification: ClassificationTransformationResult | Unset = UNSET
    generation: str | Unset = UNSET
    generation_confidence: float | Unset = UNSET
    context_relevance: float | Unset = UNSET
    followup_questions: list[str] | Unset = UNSET
    eval_result: EvalResult | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        hits = []
        for hits_item_data in self.hits:
            hits_item = hits_item_data.to_dict()
            hits.append(hits_item)

        id = self.id

        model = self.model

        created_at = self.created_at

        incomplete_details: dict[str, Any] | Unset = UNSET
        if not isinstance(self.incomplete_details, Unset):
            incomplete_details = self.incomplete_details.to_dict()

        usage: dict[str, Any] | Unset = UNSET
        if not isinstance(self.usage, Unset):
            usage = self.usage.to_dict()

        steps: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.steps, Unset):
            steps = []
            for steps_item_data in self.steps:
                steps_item = steps_item_data.to_dict()
                steps.append(steps_item)

        strategy_used: str | Unset = UNSET
        if not isinstance(self.strategy_used, Unset):
            strategy_used = self.strategy_used.value

        session_id = self.session_id

        iteration = self.iteration

        clarification_count = self.clarification_count

        remaining_internal_iterations = self.remaining_internal_iterations

        remaining_user_clarifications = self.remaining_user_clarifications

        questions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.questions, Unset):
            questions = []
            for questions_item_data in self.questions:
                questions_item = questions_item_data.to_dict()
                questions.append(questions_item)

        applied_filters: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.applied_filters, Unset):
            applied_filters = []
            for applied_filters_item_data in self.applied_filters:
                applied_filters_item = applied_filters_item_data.to_dict()
                applied_filters.append(applied_filters_item)

        tool_calls_made = self.tool_calls_made

        messages: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.messages, Unset):
            messages = []
            for messages_item_data in self.messages:
                messages_item = messages_item_data.to_dict()
                messages.append(messages_item)

        classification: dict[str, Any] | Unset = UNSET
        if not isinstance(self.classification, Unset):
            classification = self.classification.to_dict()

        generation = self.generation

        generation_confidence = self.generation_confidence

        context_relevance = self.context_relevance

        followup_questions: list[str] | Unset = UNSET
        if not isinstance(self.followup_questions, Unset):
            followup_questions = self.followup_questions

        eval_result: dict[str, Any] | Unset = UNSET
        if not isinstance(self.eval_result, Unset):
            eval_result = self.eval_result.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "status": status,
                "hits": hits,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if model is not UNSET:
            field_dict["model"] = model
        if created_at is not UNSET:
            field_dict["created_at"] = created_at
        if incomplete_details is not UNSET:
            field_dict["incomplete_details"] = incomplete_details
        if usage is not UNSET:
            field_dict["usage"] = usage
        if steps is not UNSET:
            field_dict["steps"] = steps
        if strategy_used is not UNSET:
            field_dict["strategy_used"] = strategy_used
        if session_id is not UNSET:
            field_dict["session_id"] = session_id
        if iteration is not UNSET:
            field_dict["iteration"] = iteration
        if clarification_count is not UNSET:
            field_dict["clarification_count"] = clarification_count
        if remaining_internal_iterations is not UNSET:
            field_dict["remaining_internal_iterations"] = remaining_internal_iterations
        if remaining_user_clarifications is not UNSET:
            field_dict["remaining_user_clarifications"] = remaining_user_clarifications
        if questions is not UNSET:
            field_dict["questions"] = questions
        if applied_filters is not UNSET:
            field_dict["applied_filters"] = applied_filters
        if tool_calls_made is not UNSET:
            field_dict["tool_calls_made"] = tool_calls_made
        if messages is not UNSET:
            field_dict["messages"] = messages
        if classification is not UNSET:
            field_dict["classification"] = classification
        if generation is not UNSET:
            field_dict["generation"] = generation
        if generation_confidence is not UNSET:
            field_dict["generation_confidence"] = generation_confidence
        if context_relevance is not UNSET:
            field_dict["context_relevance"] = context_relevance
        if followup_questions is not UNSET:
            field_dict["followup_questions"] = followup_questions
        if eval_result is not UNSET:
            field_dict["eval_result"] = eval_result

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.agent_question import AgentQuestion
        from ..models.agent_step import AgentStep
        from ..models.chat_message import ChatMessage
        from ..models.classification_transformation_result import ClassificationTransformationResult
        from ..models.eval_result import EvalResult
        from ..models.filter_spec import FilterSpec
        from ..models.incomplete_details import IncompleteDetails
        from ..models.query_hit import QueryHit
        from ..models.retrieval_agent_usage import RetrievalAgentUsage

        d = dict(src_dict)
        status = AgentStatus(d.pop("status"))

        hits = []
        _hits = d.pop("hits")
        for hits_item_data in _hits:
            hits_item = QueryHit.from_dict(hits_item_data)

            hits.append(hits_item)

        id = d.pop("id", UNSET)

        model = d.pop("model", UNSET)

        created_at = d.pop("created_at", UNSET)

        _incomplete_details = d.pop("incomplete_details", UNSET)
        incomplete_details: IncompleteDetails | Unset
        if isinstance(_incomplete_details, Unset):
            incomplete_details = UNSET
        else:
            incomplete_details = IncompleteDetails.from_dict(_incomplete_details)

        _usage = d.pop("usage", UNSET)
        usage: RetrievalAgentUsage | Unset
        if isinstance(_usage, Unset):
            usage = UNSET
        else:
            usage = RetrievalAgentUsage.from_dict(_usage)

        _steps = d.pop("steps", UNSET)
        steps: list[AgentStep] | Unset = UNSET
        if _steps is not UNSET:
            steps = []
            for steps_item_data in _steps:
                steps_item = AgentStep.from_dict(steps_item_data)

                steps.append(steps_item)

        _strategy_used = d.pop("strategy_used", UNSET)
        strategy_used: RetrievalStrategy | Unset
        if isinstance(_strategy_used, Unset):
            strategy_used = UNSET
        else:
            strategy_used = RetrievalStrategy(_strategy_used)

        session_id = d.pop("session_id", UNSET)

        iteration = d.pop("iteration", UNSET)

        clarification_count = d.pop("clarification_count", UNSET)

        remaining_internal_iterations = d.pop("remaining_internal_iterations", UNSET)

        remaining_user_clarifications = d.pop("remaining_user_clarifications", UNSET)

        _questions = d.pop("questions", UNSET)
        questions: list[AgentQuestion] | Unset = UNSET
        if _questions is not UNSET:
            questions = []
            for questions_item_data in _questions:
                questions_item = AgentQuestion.from_dict(questions_item_data)

                questions.append(questions_item)

        _applied_filters = d.pop("applied_filters", UNSET)
        applied_filters: list[FilterSpec] | Unset = UNSET
        if _applied_filters is not UNSET:
            applied_filters = []
            for applied_filters_item_data in _applied_filters:
                applied_filters_item = FilterSpec.from_dict(applied_filters_item_data)

                applied_filters.append(applied_filters_item)

        tool_calls_made = d.pop("tool_calls_made", UNSET)

        _messages = d.pop("messages", UNSET)
        messages: list[ChatMessage] | Unset = UNSET
        if _messages is not UNSET:
            messages = []
            for messages_item_data in _messages:
                messages_item = ChatMessage.from_dict(messages_item_data)

                messages.append(messages_item)

        _classification = d.pop("classification", UNSET)
        classification: ClassificationTransformationResult | Unset
        if isinstance(_classification, Unset):
            classification = UNSET
        else:
            classification = ClassificationTransformationResult.from_dict(_classification)

        generation = d.pop("generation", UNSET)

        generation_confidence = d.pop("generation_confidence", UNSET)

        context_relevance = d.pop("context_relevance", UNSET)

        followup_questions = cast(list[str], d.pop("followup_questions", UNSET))

        _eval_result = d.pop("eval_result", UNSET)
        eval_result: EvalResult | Unset
        if isinstance(_eval_result, Unset):
            eval_result = UNSET
        else:
            eval_result = EvalResult.from_dict(_eval_result)

        retrieval_agent_result = cls(
            status=status,
            hits=hits,
            id=id,
            model=model,
            created_at=created_at,
            incomplete_details=incomplete_details,
            usage=usage,
            steps=steps,
            strategy_used=strategy_used,
            session_id=session_id,
            iteration=iteration,
            clarification_count=clarification_count,
            remaining_internal_iterations=remaining_internal_iterations,
            remaining_user_clarifications=remaining_user_clarifications,
            questions=questions,
            applied_filters=applied_filters,
            tool_calls_made=tool_calls_made,
            messages=messages,
            classification=classification,
            generation=generation,
            generation_confidence=generation_confidence,
            context_relevance=context_relevance,
            followup_questions=followup_questions,
            eval_result=eval_result,
        )

        retrieval_agent_result.additional_properties = d
        return retrieval_agent_result

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
