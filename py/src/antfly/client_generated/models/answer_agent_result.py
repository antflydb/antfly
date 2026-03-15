from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.classification_transformation_result import ClassificationTransformationResult
    from ..models.eval_result import EvalResult
    from ..models.query_result import QueryResult


T = TypeVar("T", bound="AnswerAgentResult")


@_attrs_define
class AnswerAgentResult:
    """DEPRECATED: Use RetrievalAgentResult instead.
    Result from the answer agent.

        Attributes:
            answer (Union[Unset, str]): DEPRECATED: Use generation on RetrievalAgentResult instead.
                Generated answer in markdown format.
            answer_confidence (Union[Unset, float]): DEPRECATED: Use generation_confidence on RetrievalAgentResult instead.
                Confidence in the generated answer.
            context_relevance (Union[Unset, float]): Relevance of retrieved documents to the query
            classification_transformation (Union[Unset, ClassificationTransformationResult]): Query classification and
                transformation result combining all query enhancements including strategy selection and semantic optimization
            query_results (Union[Unset, list['QueryResult']]): DEPRECATED: Use hits on RetrievalAgentResult instead.
                Query results grouped by table.
            followup_questions (Union[Unset, list[str]]): Suggested follow-up questions
            eval_result (Union[Unset, EvalResult]): Complete evaluation result
    """

    answer: Union[Unset, str] = UNSET
    answer_confidence: Union[Unset, float] = UNSET
    context_relevance: Union[Unset, float] = UNSET
    classification_transformation: Union[Unset, "ClassificationTransformationResult"] = UNSET
    query_results: Union[Unset, list["QueryResult"]] = UNSET
    followup_questions: Union[Unset, list[str]] = UNSET
    eval_result: Union[Unset, "EvalResult"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        answer = self.answer

        answer_confidence = self.answer_confidence

        context_relevance = self.context_relevance

        classification_transformation: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.classification_transformation, Unset):
            classification_transformation = self.classification_transformation.to_dict()

        query_results: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.query_results, Unset):
            query_results = []
            for query_results_item_data in self.query_results:
                query_results_item = query_results_item_data.to_dict()
                query_results.append(query_results_item)

        followup_questions: Union[Unset, list[str]] = UNSET
        if not isinstance(self.followup_questions, Unset):
            followup_questions = self.followup_questions

        eval_result: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.eval_result, Unset):
            eval_result = self.eval_result.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if answer is not UNSET:
            field_dict["answer"] = answer
        if answer_confidence is not UNSET:
            field_dict["answer_confidence"] = answer_confidence
        if context_relevance is not UNSET:
            field_dict["context_relevance"] = context_relevance
        if classification_transformation is not UNSET:
            field_dict["classification_transformation"] = classification_transformation
        if query_results is not UNSET:
            field_dict["query_results"] = query_results
        if followup_questions is not UNSET:
            field_dict["followup_questions"] = followup_questions
        if eval_result is not UNSET:
            field_dict["eval_result"] = eval_result

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.classification_transformation_result import ClassificationTransformationResult
        from ..models.eval_result import EvalResult
        from ..models.query_result import QueryResult

        d = dict(src_dict)
        answer = d.pop("answer", UNSET)

        answer_confidence = d.pop("answer_confidence", UNSET)

        context_relevance = d.pop("context_relevance", UNSET)

        _classification_transformation = d.pop("classification_transformation", UNSET)
        classification_transformation: Union[Unset, ClassificationTransformationResult]
        if isinstance(_classification_transformation, Unset):
            classification_transformation = UNSET
        else:
            classification_transformation = ClassificationTransformationResult.from_dict(_classification_transformation)

        query_results = []
        _query_results = d.pop("query_results", UNSET)
        for query_results_item_data in _query_results or []:
            query_results_item = QueryResult.from_dict(query_results_item_data)

            query_results.append(query_results_item)

        followup_questions = cast(list[str], d.pop("followup_questions", UNSET))

        _eval_result = d.pop("eval_result", UNSET)
        eval_result: Union[Unset, EvalResult]
        if isinstance(_eval_result, Unset):
            eval_result = UNSET
        else:
            eval_result = EvalResult.from_dict(_eval_result)

        answer_agent_result = cls(
            answer=answer,
            answer_confidence=answer_confidence,
            context_relevance=context_relevance,
            classification_transformation=classification_transformation,
            query_results=query_results,
            followup_questions=followup_questions,
            eval_result=eval_result,
        )

        answer_agent_result.additional_properties = d
        return answer_agent_result

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
