from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_builder_result_query import QueryBuilderResultQuery


T = TypeVar("T", bound="QueryBuilderResult")


@_attrs_define
class QueryBuilderResult:
    """
    Attributes:
        query (QueryBuilderResultQuery): Generated search query in simplified DSL format.
            Can be used directly in QueryRequest.full_text_search or filter_query.
             Example: {'and': [{'match': 'machine learning', 'field': 'content'}, {'term': 'published', 'field':
            'status'}]}.
        explanation (Union[Unset, str]): Human-readable explanation of what the query does and why it was structured
            this way Example: Searches for 'machine learning' in content field AND requires status to be exactly
            'published'.
        confidence (Union[Unset, float]): Model's confidence in the generated query (0.0-1.0) Example: 0.85.
        warnings (Union[Unset, list[str]]): Any issues, limitations, or assumptions made when generating the query
            Example: ["Field 'category' not found in schema, using content field instead"].
    """

    query: "QueryBuilderResultQuery"
    explanation: Union[Unset, str] = UNSET
    confidence: Union[Unset, float] = UNSET
    warnings: Union[Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        query = self.query.to_dict()

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
        if explanation is not UNSET:
            field_dict["explanation"] = explanation
        if confidence is not UNSET:
            field_dict["confidence"] = confidence
        if warnings is not UNSET:
            field_dict["warnings"] = warnings

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_builder_result_query import QueryBuilderResultQuery

        d = dict(src_dict)
        query = QueryBuilderResultQuery.from_dict(d.pop("query"))

        explanation = d.pop("explanation", UNSET)

        confidence = d.pop("confidence", UNSET)

        warnings = cast(list[str], d.pop("warnings", UNSET))

        query_builder_result = cls(
            query=query,
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
