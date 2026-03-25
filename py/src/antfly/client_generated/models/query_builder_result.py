from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.agent_status import AgentStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.agent_question import AgentQuestion
    from ..models.agent_step import AgentStep
    from ..models.query_builder_result_query import QueryBuilderResultQuery


T = TypeVar("T", bound="QueryBuilderResult")


@_attrs_define
class QueryBuilderResult:
    """
    Attributes:
        query (QueryBuilderResultQuery): Generated search query in native Bleve format.
            Can be used directly in QueryRequest.full_text_search or filter_query.
             Example: {'conjuncts': [{'match': 'machine learning', 'field': 'content'}, {'term': 'published', 'field':
            'status'}]}.
        session_id (Union[Unset, str]): Correlation identifier for client-carried continuation.
        iteration (Union[Unset, int]): Number of internal passes consumed while producing this result.
        clarification_count (Union[Unset, int]): Number of user clarification turns already consumed in this
            interaction.
        status (Union[Unset, AgentStatus]): Shared bounded-agent execution status
        steps (Union[Unset, list['AgentStep']]): Shared bounded-agent execution trace for this query-builder run.
        remaining_internal_iterations (Union[Unset, int]): Remaining internal reasoning passes for this interaction.
        remaining_user_clarifications (Union[Unset, int]): Remaining clarification turns allowed for this interaction.
        questions (Union[Unset, list['AgentQuestion']]): Clarification questions exposed in the shared bounded-agent
            envelope.
        explanation (Union[Unset, str]): Human-readable explanation of what the query does and why it was structured
            this way Example: Searches for 'machine learning' in content field AND requires status to be exactly
            'published'.
        confidence (Union[Unset, float]): Model's confidence in the generated query (0.0-1.0) Example: 0.85.
        warnings (Union[Unset, list[str]]): Any issues, limitations, or assumptions made when generating the query
            Example: ["Field 'category' not found in schema, using content field instead"].
    """

    query: "QueryBuilderResultQuery"
    session_id: Union[Unset, str] = UNSET
    iteration: Union[Unset, int] = UNSET
    clarification_count: Union[Unset, int] = UNSET
    status: Union[Unset, AgentStatus] = UNSET
    steps: Union[Unset, list["AgentStep"]] = UNSET
    remaining_internal_iterations: Union[Unset, int] = UNSET
    remaining_user_clarifications: Union[Unset, int] = UNSET
    questions: Union[Unset, list["AgentQuestion"]] = UNSET
    explanation: Union[Unset, str] = UNSET
    confidence: Union[Unset, float] = UNSET
    warnings: Union[Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        query = self.query.to_dict()

        session_id = self.session_id

        iteration = self.iteration

        clarification_count = self.clarification_count

        status: Union[Unset, str] = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        steps: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.steps, Unset):
            steps = []
            for steps_item_data in self.steps:
                steps_item = steps_item_data.to_dict()
                steps.append(steps_item)

        remaining_internal_iterations = self.remaining_internal_iterations

        remaining_user_clarifications = self.remaining_user_clarifications

        questions: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.questions, Unset):
            questions = []
            for questions_item_data in self.questions:
                questions_item = questions_item_data.to_dict()
                questions.append(questions_item)

        explanation = self.explanation

        confidence = self.confidence

        warnings: Union[Unset, list[str]] = UNSET
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
        from ..models.query_builder_result_query import QueryBuilderResultQuery

        d = dict(src_dict)
        query = QueryBuilderResultQuery.from_dict(d.pop("query"))

        session_id = d.pop("session_id", UNSET)

        iteration = d.pop("iteration", UNSET)

        clarification_count = d.pop("clarification_count", UNSET)

        _status = d.pop("status", UNSET)
        status: Union[Unset, AgentStatus]
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = AgentStatus(_status)

        steps = []
        _steps = d.pop("steps", UNSET)
        for steps_item_data in _steps or []:
            steps_item = AgentStep.from_dict(steps_item_data)

            steps.append(steps_item)

        remaining_internal_iterations = d.pop("remaining_internal_iterations", UNSET)

        remaining_user_clarifications = d.pop("remaining_user_clarifications", UNSET)

        questions = []
        _questions = d.pop("questions", UNSET)
        for questions_item_data in _questions or []:
            questions_item = AgentQuestion.from_dict(questions_item_data)

            questions.append(questions_item)

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
