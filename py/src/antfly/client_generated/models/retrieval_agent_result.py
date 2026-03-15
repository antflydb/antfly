from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.retrieval_agent_status import RetrievalAgentStatus
from ..models.retrieval_strategy import RetrievalStrategy
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chat_message import ChatMessage
    from ..models.clarification_request import ClarificationRequest
    from ..models.classification_transformation_result import ClassificationTransformationResult
    from ..models.eval_result import EvalResult
    from ..models.filter_spec import FilterSpec
    from ..models.incomplete_details import IncompleteDetails
    from ..models.query_hit import QueryHit
    from ..models.retrieval_agent_usage import RetrievalAgentUsage
    from ..models.retrieval_reasoning_step import RetrievalReasoningStep


T = TypeVar("T", bound="RetrievalAgentResult")


@_attrs_define
class RetrievalAgentResult:
    """Result from the retrieval agent

    Attributes:
        status (RetrievalAgentStatus): Current status of the retrieval agent execution:
            - completed: Agent finished successfully
            - in_progress: Agent is still executing (streaming context)
            - incomplete: Agent stopped before completion (see incomplete_details)
            - failed: Error occurred during execution
        hits (list['QueryHit']): Retrieved query hits
        id (Union[Unset, str]): Unique response ID for logging and tracing Example: ragr_cr3ig20h5tbs73e3ahrg.
        model (Union[Unset, str]): LLM model used for generation Example: gemini-2.0-flash.
        created_at (Union[Unset, int]): Unix timestamp (seconds) when the response was created
        incomplete_details (Union[Unset, IncompleteDetails]): Explains why the agent stopped before completion. Present
            when status is "incomplete".
        usage (Union[Unset, RetrievalAgentUsage]): Token usage and resource statistics from the retrieval agent
            execution
        reasoning_chain (Union[Unset, list['RetrievalReasoningStep']]): Steps taken during retrieval (tool calls,
            actions)
        strategy_used (Union[Unset, RetrievalStrategy]): Strategy for document retrieval:
            - semantic: Vector similarity search using embeddings
            - bm25: Full-text search using BM25 scoring
            - metadata: Structured query on document fields
            - tree: Iterative tree navigation with summarization
            - graph: Relationship-based traversal
            - hybrid: Combine multiple strategies with RRF or rerank
        clarification_request (Union[Unset, ClarificationRequest]): Request for clarification from the user
        applied_filters (Union[Unset, list['FilterSpec']]): Filters that were applied during retrieval
        tool_calls_made (Union[Unset, int]): Total number of tool calls made during retrieval
        messages (Union[Unset, list['ChatMessage']]): Conversation messages including tool calls and responses.
            Can be passed back in subsequent requests for multi-turn interaction.
        classification (Union[Unset, ClassificationTransformationResult]): Query classification and transformation
            result combining all query enhancements including strategy selection and semantic optimization
        generation (Union[Unset, str]): Generated response in markdown format. Present when steps.generation
            was configured.
        generation_confidence (Union[Unset, float]): Confidence in the generated response (requires steps.confidence)
        context_relevance (Union[Unset, float]): Relevance of retrieved documents to the query (requires
            steps.confidence)
        followup_questions (Union[Unset, list[str]]): Suggested follow-up questions (requires steps.followup)
        eval_result (Union[Unset, EvalResult]): Complete evaluation result
    """

    status: RetrievalAgentStatus
    hits: list["QueryHit"]
    id: Union[Unset, str] = UNSET
    model: Union[Unset, str] = UNSET
    created_at: Union[Unset, int] = UNSET
    incomplete_details: Union[Unset, "IncompleteDetails"] = UNSET
    usage: Union[Unset, "RetrievalAgentUsage"] = UNSET
    reasoning_chain: Union[Unset, list["RetrievalReasoningStep"]] = UNSET
    strategy_used: Union[Unset, RetrievalStrategy] = UNSET
    clarification_request: Union[Unset, "ClarificationRequest"] = UNSET
    applied_filters: Union[Unset, list["FilterSpec"]] = UNSET
    tool_calls_made: Union[Unset, int] = UNSET
    messages: Union[Unset, list["ChatMessage"]] = UNSET
    classification: Union[Unset, "ClassificationTransformationResult"] = UNSET
    generation: Union[Unset, str] = UNSET
    generation_confidence: Union[Unset, float] = UNSET
    context_relevance: Union[Unset, float] = UNSET
    followup_questions: Union[Unset, list[str]] = UNSET
    eval_result: Union[Unset, "EvalResult"] = UNSET
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

        incomplete_details: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.incomplete_details, Unset):
            incomplete_details = self.incomplete_details.to_dict()

        usage: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.usage, Unset):
            usage = self.usage.to_dict()

        reasoning_chain: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.reasoning_chain, Unset):
            reasoning_chain = []
            for reasoning_chain_item_data in self.reasoning_chain:
                reasoning_chain_item = reasoning_chain_item_data.to_dict()
                reasoning_chain.append(reasoning_chain_item)

        strategy_used: Union[Unset, str] = UNSET
        if not isinstance(self.strategy_used, Unset):
            strategy_used = self.strategy_used.value

        clarification_request: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.clarification_request, Unset):
            clarification_request = self.clarification_request.to_dict()

        applied_filters: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.applied_filters, Unset):
            applied_filters = []
            for applied_filters_item_data in self.applied_filters:
                applied_filters_item = applied_filters_item_data.to_dict()
                applied_filters.append(applied_filters_item)

        tool_calls_made = self.tool_calls_made

        messages: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.messages, Unset):
            messages = []
            for messages_item_data in self.messages:
                messages_item = messages_item_data.to_dict()
                messages.append(messages_item)

        classification: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.classification, Unset):
            classification = self.classification.to_dict()

        generation = self.generation

        generation_confidence = self.generation_confidence

        context_relevance = self.context_relevance

        followup_questions: Union[Unset, list[str]] = UNSET
        if not isinstance(self.followup_questions, Unset):
            followup_questions = self.followup_questions

        eval_result: Union[Unset, dict[str, Any]] = UNSET
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
        if reasoning_chain is not UNSET:
            field_dict["reasoning_chain"] = reasoning_chain
        if strategy_used is not UNSET:
            field_dict["strategy_used"] = strategy_used
        if clarification_request is not UNSET:
            field_dict["clarification_request"] = clarification_request
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
        from ..models.chat_message import ChatMessage
        from ..models.clarification_request import ClarificationRequest
        from ..models.classification_transformation_result import ClassificationTransformationResult
        from ..models.eval_result import EvalResult
        from ..models.filter_spec import FilterSpec
        from ..models.incomplete_details import IncompleteDetails
        from ..models.query_hit import QueryHit
        from ..models.retrieval_agent_usage import RetrievalAgentUsage
        from ..models.retrieval_reasoning_step import RetrievalReasoningStep

        d = dict(src_dict)
        status = RetrievalAgentStatus(d.pop("status"))

        hits = []
        _hits = d.pop("hits")
        for hits_item_data in _hits:
            hits_item = QueryHit.from_dict(hits_item_data)

            hits.append(hits_item)

        id = d.pop("id", UNSET)

        model = d.pop("model", UNSET)

        created_at = d.pop("created_at", UNSET)

        _incomplete_details = d.pop("incomplete_details", UNSET)
        incomplete_details: Union[Unset, IncompleteDetails]
        if isinstance(_incomplete_details, Unset):
            incomplete_details = UNSET
        else:
            incomplete_details = IncompleteDetails.from_dict(_incomplete_details)

        _usage = d.pop("usage", UNSET)
        usage: Union[Unset, RetrievalAgentUsage]
        if isinstance(_usage, Unset):
            usage = UNSET
        else:
            usage = RetrievalAgentUsage.from_dict(_usage)

        reasoning_chain = []
        _reasoning_chain = d.pop("reasoning_chain", UNSET)
        for reasoning_chain_item_data in _reasoning_chain or []:
            reasoning_chain_item = RetrievalReasoningStep.from_dict(reasoning_chain_item_data)

            reasoning_chain.append(reasoning_chain_item)

        _strategy_used = d.pop("strategy_used", UNSET)
        strategy_used: Union[Unset, RetrievalStrategy]
        if isinstance(_strategy_used, Unset):
            strategy_used = UNSET
        else:
            strategy_used = RetrievalStrategy(_strategy_used)

        _clarification_request = d.pop("clarification_request", UNSET)
        clarification_request: Union[Unset, ClarificationRequest]
        if isinstance(_clarification_request, Unset):
            clarification_request = UNSET
        else:
            clarification_request = ClarificationRequest.from_dict(_clarification_request)

        applied_filters = []
        _applied_filters = d.pop("applied_filters", UNSET)
        for applied_filters_item_data in _applied_filters or []:
            applied_filters_item = FilterSpec.from_dict(applied_filters_item_data)

            applied_filters.append(applied_filters_item)

        tool_calls_made = d.pop("tool_calls_made", UNSET)

        messages = []
        _messages = d.pop("messages", UNSET)
        for messages_item_data in _messages or []:
            messages_item = ChatMessage.from_dict(messages_item_data)

            messages.append(messages_item)

        _classification = d.pop("classification", UNSET)
        classification: Union[Unset, ClassificationTransformationResult]
        if isinstance(_classification, Unset):
            classification = UNSET
        else:
            classification = ClassificationTransformationResult.from_dict(_classification)

        generation = d.pop("generation", UNSET)

        generation_confidence = d.pop("generation_confidence", UNSET)

        context_relevance = d.pop("context_relevance", UNSET)

        followup_questions = cast(list[str], d.pop("followup_questions", UNSET))

        _eval_result = d.pop("eval_result", UNSET)
        eval_result: Union[Unset, EvalResult]
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
            reasoning_chain=reasoning_chain,
            strategy_used=strategy_used,
            clarification_request=clarification_request,
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
