from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.agent_status import AgentStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.agent_question import AgentQuestion
    from ..models.agent_step import AgentStep
    from ..models.query_builder_result_plan import QueryBuilderResultPlan
    from ..models.query_builder_result_query import QueryBuilderResultQuery
    from ..models.query_request import QueryRequest
    from ..models.retrieval_query_request import RetrievalQueryRequest


T = TypeVar("T", bound="QueryBuilderResult")


@_attrs_define
class QueryBuilderResult:
    """
    Attributes:
        query (QueryBuilderResultQuery): Generated search query in native Bleve format.
            Can be used directly in QueryRequest.full_text_search or filter_query.
             Example: {'conjuncts': [{'match': 'machine learning', 'field': 'content'}, {'term': 'published', 'field':
            'status'}]}.
        session_id (str | Unset): Correlation identifier for client-carried continuation.
        iteration (int | Unset): Number of internal passes consumed while producing this result.
        clarification_count (int | Unset): Number of user clarification turns already consumed in this interaction.
        status (AgentStatus | Unset): Shared bounded-agent execution status
        steps (list[AgentStep] | Unset): Shared bounded-agent execution trace for this query-builder run.
        remaining_internal_iterations (int | Unset): Remaining internal reasoning passes for this interaction.
        remaining_user_clarifications (int | Unset): Remaining clarification turns allowed for this interaction.
        questions (list[AgentQuestion] | Unset): Clarification questions exposed in the shared bounded-agent envelope.
        query_request (QueryRequest | Unset):
        retrieval_query_request (RetrievalQueryRequest | Unset): A query in the retrieval pipeline. Extends QueryRequest
            with an optional
            tree search configuration. Each query specifies its own table.

            When both search fields (semantic_search, full_text_search) and tree_search
            are provided, the search results are used as start nodes for tree navigation.
        specialist (str | Unset): Specialist or strategy used to build the query, such as `full_text`, `filter`, or
            `hybrid`. Example: full_text.
        plan (QueryBuilderResultPlan | Unset): Optional machine-readable coordination plan for observability.
        explanation (str | Unset): Human-readable explanation of what the query does and why it was structured this way
            Example: Searches for 'machine learning' in content field AND requires status to be exactly 'published'.
        confidence (float | Unset): Model's confidence in the generated query (0.0-1.0) Example: 0.85.
        warnings (list[str] | Unset): Any issues, limitations, or assumptions made when generating the query Example:
            ["Field 'category' not found in schema, using content field instead"].
    """

    query: QueryBuilderResultQuery
    session_id: str | Unset = UNSET
    iteration: int | Unset = UNSET
    clarification_count: int | Unset = UNSET
    status: AgentStatus | Unset = UNSET
    steps: list[AgentStep] | Unset = UNSET
    remaining_internal_iterations: int | Unset = UNSET
    remaining_user_clarifications: int | Unset = UNSET
    questions: list[AgentQuestion] | Unset = UNSET
    query_request: QueryRequest | Unset = UNSET
    retrieval_query_request: RetrievalQueryRequest | Unset = UNSET
    specialist: str | Unset = UNSET
    plan: QueryBuilderResultPlan | Unset = UNSET
    explanation: str | Unset = UNSET
    confidence: float | Unset = UNSET
    warnings: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        query = self.query.to_dict()

        session_id = self.session_id

        iteration = self.iteration

        clarification_count = self.clarification_count

        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        steps: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.steps, Unset):
            steps = []
            for steps_item_data in self.steps:
                steps_item = steps_item_data.to_dict()
                steps.append(steps_item)

        remaining_internal_iterations = self.remaining_internal_iterations

        remaining_user_clarifications = self.remaining_user_clarifications

        questions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.questions, Unset):
            questions = []
            for questions_item_data in self.questions:
                questions_item = questions_item_data.to_dict()
                questions.append(questions_item)

        query_request: dict[str, Any] | Unset = UNSET
        if not isinstance(self.query_request, Unset):
            query_request = self.query_request.to_dict()

        retrieval_query_request: dict[str, Any] | Unset = UNSET
        if not isinstance(self.retrieval_query_request, Unset):
            retrieval_query_request = self.retrieval_query_request.to_dict()

        specialist = self.specialist

        plan: dict[str, Any] | Unset = UNSET
        if not isinstance(self.plan, Unset):
            plan = self.plan.to_dict()

        explanation = self.explanation

        confidence = self.confidence

        warnings: list[str] | Unset = UNSET
        if not isinstance(self.warnings, Unset):
            warnings = self.warnings

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "query": query,
            }
        )
        if session_id is not UNSET:
            field_dict["session_id"] = session_id
        if iteration is not UNSET:
            field_dict["iteration"] = iteration
        if clarification_count is not UNSET:
            field_dict["clarification_count"] = clarification_count
        if status is not UNSET:
            field_dict["status"] = status
        if steps is not UNSET:
            field_dict["steps"] = steps
        if remaining_internal_iterations is not UNSET:
            field_dict["remaining_internal_iterations"] = remaining_internal_iterations
        if remaining_user_clarifications is not UNSET:
            field_dict["remaining_user_clarifications"] = remaining_user_clarifications
        if questions is not UNSET:
            field_dict["questions"] = questions
        if query_request is not UNSET:
            field_dict["query_request"] = query_request
        if retrieval_query_request is not UNSET:
            field_dict["retrieval_query_request"] = retrieval_query_request
        if specialist is not UNSET:
            field_dict["specialist"] = specialist
        if plan is not UNSET:
            field_dict["plan"] = plan
        if explanation is not UNSET:
            field_dict["explanation"] = explanation
        if confidence is not UNSET:
            field_dict["confidence"] = confidence
        if warnings is not UNSET:
            field_dict["warnings"] = warnings

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.agent_question import AgentQuestion
        from ..models.agent_step import AgentStep
        from ..models.query_builder_result_plan import QueryBuilderResultPlan
        from ..models.query_builder_result_query import QueryBuilderResultQuery
        from ..models.query_request import QueryRequest
        from ..models.retrieval_query_request import RetrievalQueryRequest

        d = dict(src_dict)
        query = QueryBuilderResultQuery.from_dict(d.pop("query"))

        session_id = d.pop("session_id", UNSET)

        iteration = d.pop("iteration", UNSET)

        clarification_count = d.pop("clarification_count", UNSET)

        _status = d.pop("status", UNSET)
        status: AgentStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = AgentStatus(_status)

        _steps = d.pop("steps", UNSET)
        steps: list[AgentStep] | Unset = UNSET
        if _steps is not UNSET:
            steps = []
            for steps_item_data in _steps:
                steps_item = AgentStep.from_dict(steps_item_data)

                steps.append(steps_item)

        remaining_internal_iterations = d.pop("remaining_internal_iterations", UNSET)

        remaining_user_clarifications = d.pop("remaining_user_clarifications", UNSET)

        _questions = d.pop("questions", UNSET)
        questions: list[AgentQuestion] | Unset = UNSET
        if _questions is not UNSET:
            questions = []
            for questions_item_data in _questions:
                questions_item = AgentQuestion.from_dict(questions_item_data)

                questions.append(questions_item)

        _query_request = d.pop("query_request", UNSET)
        query_request: QueryRequest | Unset
        if isinstance(_query_request, Unset):
            query_request = UNSET
        else:
            query_request = QueryRequest.from_dict(_query_request)

        _retrieval_query_request = d.pop("retrieval_query_request", UNSET)
        retrieval_query_request: RetrievalQueryRequest | Unset
        if isinstance(_retrieval_query_request, Unset):
            retrieval_query_request = UNSET
        else:
            retrieval_query_request = RetrievalQueryRequest.from_dict(_retrieval_query_request)

        specialist = d.pop("specialist", UNSET)

        _plan = d.pop("plan", UNSET)
        plan: QueryBuilderResultPlan | Unset
        if isinstance(_plan, Unset):
            plan = UNSET
        else:
            plan = QueryBuilderResultPlan.from_dict(_plan)

        explanation = d.pop("explanation", UNSET)

        confidence = d.pop("confidence", UNSET)

        warnings = cast(list[str], d.pop("warnings", UNSET))

        query_builder_result = cls(
            query=query,
            session_id=session_id,
            iteration=iteration,
            clarification_count=clarification_count,
            status=status,
            steps=steps,
            remaining_internal_iterations=remaining_internal_iterations,
            remaining_user_clarifications=remaining_user_clarifications,
            questions=questions,
            query_request=query_request,
            retrieval_query_request=retrieval_query_request,
            specialist=specialist,
            plan=plan,
            explanation=explanation,
            confidence=confidence,
            warnings=warnings,
        )

        query_builder_result.additional_properties = d
        return query_builder_result

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
