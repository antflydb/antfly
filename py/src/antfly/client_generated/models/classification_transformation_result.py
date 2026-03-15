from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_strategy import QueryStrategy
from ..models.route_type import RouteType
from ..models.semantic_query_mode import SemanticQueryMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="ClassificationTransformationResult")


@_attrs_define
class ClassificationTransformationResult:
    """Query classification and transformation result combining all query enhancements including strategy selection and
    semantic optimization

        Attributes:
            route_type (RouteType): Classification of query type: question (specific factual query) or search (exploratory
                query)
            strategy (QueryStrategy): Strategy for query transformation and retrieval:
                - simple: Direct query with multi-phrase expansion. Best for straightforward factual queries.
                - decompose: Break complex queries into sub-questions, retrieve for each. Best for multi-part questions.
                - step_back: Generate broader background query first, then specific query. Best for questions needing context.
                - hyde: Generate hypothetical answer document, embed that for retrieval. Best for abstract/conceptual questions.
            semantic_mode (SemanticQueryMode): Mode for semantic query generation:
                - rewrite: Transform query into expanded keywords/concepts optimized for vector search (Level 2 optimization)
                - hypothetical: Generate a hypothetical answer that would appear in relevant documents (HyDE - Level 3
                optimization)
            improved_query (str): Clarified query with added context for answer generation (human-readable)
            semantic_query (str): Optimized query for vector/semantic search. Content style depends on semantic_mode:
                keywords for 'rewrite', hypothetical answer for 'hypothetical'
            confidence (float): Classification confidence (0.0 to 1.0)
            step_back_query (Union[Unset, str]): Broader background query for context (only present when strategy is
                'step_back')
            sub_questions (Union[Unset, list[str]]): Decomposed sub-questions (only present when strategy is 'decompose')
            multi_phrases (Union[Unset, list[str]]): Alternative phrasings of the query for expanded retrieval coverage
            reasoning (Union[Unset, str]): Pre-retrieval reasoning explaining query analysis and strategy selection (only
                present when with_classification_reasoning is enabled)
    """

    route_type: RouteType
    strategy: QueryStrategy
    semantic_mode: SemanticQueryMode
    improved_query: str
    semantic_query: str
    confidence: float
    step_back_query: Union[Unset, str] = UNSET
    sub_questions: Union[Unset, list[str]] = UNSET
    multi_phrases: Union[Unset, list[str]] = UNSET
    reasoning: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        route_type = self.route_type.value

        strategy = self.strategy.value

        semantic_mode = self.semantic_mode.value

        improved_query = self.improved_query

        semantic_query = self.semantic_query

        confidence = self.confidence

        step_back_query = self.step_back_query

        sub_questions: Union[Unset, list[str]] = UNSET
        if not isinstance(self.sub_questions, Unset):
            sub_questions = self.sub_questions

        multi_phrases: Union[Unset, list[str]] = UNSET
        if not isinstance(self.multi_phrases, Unset):
            multi_phrases = self.multi_phrases

        reasoning = self.reasoning

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "route_type": route_type,
                "strategy": strategy,
                "semantic_mode": semantic_mode,
                "improved_query": improved_query,
                "semantic_query": semantic_query,
                "confidence": confidence,
            }
        )
        if step_back_query is not UNSET:
            field_dict["step_back_query"] = step_back_query
        if sub_questions is not UNSET:
            field_dict["sub_questions"] = sub_questions
        if multi_phrases is not UNSET:
            field_dict["multi_phrases"] = multi_phrases
        if reasoning is not UNSET:
            field_dict["reasoning"] = reasoning

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        route_type = RouteType(d.pop("route_type"))

        strategy = QueryStrategy(d.pop("strategy"))

        semantic_mode = SemanticQueryMode(d.pop("semantic_mode"))

        improved_query = d.pop("improved_query")

        semantic_query = d.pop("semantic_query")

        confidence = d.pop("confidence")

        step_back_query = d.pop("step_back_query", UNSET)

        sub_questions = cast(list[str], d.pop("sub_questions", UNSET))

        multi_phrases = cast(list[str], d.pop("multi_phrases", UNSET))

        reasoning = d.pop("reasoning", UNSET)

        classification_transformation_result = cls(
            route_type=route_type,
            strategy=strategy,
            semantic_mode=semantic_mode,
            improved_query=improved_query,
            semantic_query=semantic_query,
            confidence=confidence,
            step_back_query=step_back_query,
            sub_questions=sub_questions,
            multi_phrases=multi_phrases,
            reasoning=reasoning,
        )

        classification_transformation_result.additional_properties = d
        return classification_transformation_result

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
