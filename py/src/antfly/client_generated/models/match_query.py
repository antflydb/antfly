from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.fuzziness_type_1 import FuzzinessType1
from ..models.match_query_operator import MatchQueryOperator
from ..types import UNSET, Unset

T = TypeVar("T", bound="MatchQuery")


@_attrs_define
class MatchQuery:
    """
    Attributes:
        match (str):
        field (Union[Unset, str]):
        analyzer (Union[Unset, str]):
        boost (Union[None, Unset, float]): A floating-point number used to decrease or increase the relevance scores of
            a query.
        prefix_length (Union[Unset, int]):
        fuzziness (Union[FuzzinessType1, Unset, int]): The fuzziness of the query. Can be an integer or "auto".
        operator (Union[Unset, MatchQueryOperator]):
    """

    match: str
    field: Union[Unset, str] = UNSET
    analyzer: Union[Unset, str] = UNSET
    boost: Union[None, Unset, float] = UNSET
    prefix_length: Union[Unset, int] = UNSET
    fuzziness: Union[FuzzinessType1, Unset, int] = UNSET
    operator: Union[Unset, MatchQueryOperator] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        match = self.match

        field = self.field

        analyzer = self.analyzer

        boost: Union[None, Unset, float]
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        prefix_length = self.prefix_length

        fuzziness: Union[Unset, int, str]
        if isinstance(self.fuzziness, Unset):
            fuzziness = UNSET
        elif isinstance(self.fuzziness, FuzzinessType1):
            fuzziness = self.fuzziness.value
        else:
            fuzziness = self.fuzziness

        operator: Union[Unset, str] = UNSET
        if not isinstance(self.operator, Unset):
            operator = self.operator.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "match": match,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if analyzer is not UNSET:
            field_dict["analyzer"] = analyzer
        if boost is not UNSET:
            field_dict["boost"] = boost
        if prefix_length is not UNSET:
            field_dict["prefix_length"] = prefix_length
        if fuzziness is not UNSET:
            field_dict["fuzziness"] = fuzziness
        if operator is not UNSET:
            field_dict["operator"] = operator

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        match = d.pop("match")

        field = d.pop("field", UNSET)

        analyzer = d.pop("analyzer", UNSET)

        def _parse_boost(data: object) -> Union[None, Unset, float]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, float], data)

        boost = _parse_boost(d.pop("boost", UNSET))

        prefix_length = d.pop("prefix_length", UNSET)

        def _parse_fuzziness(data: object) -> Union[FuzzinessType1, Unset, int]:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                componentsschemas_fuzziness_type_1 = FuzzinessType1(data)

                return componentsschemas_fuzziness_type_1
            except:  # noqa: E722
                pass
            return cast(Union[FuzzinessType1, Unset, int], data)

        fuzziness = _parse_fuzziness(d.pop("fuzziness", UNSET))

        _operator = d.pop("operator", UNSET)
        operator: Union[Unset, MatchQueryOperator]
        if isinstance(_operator, Unset):
            operator = UNSET
        else:
            operator = MatchQueryOperator(_operator)

        match_query = cls(
            match=match,
            field=field,
            analyzer=analyzer,
            boost=boost,
            prefix_length=prefix_length,
            fuzziness=fuzziness,
            operator=operator,
        )

        match_query.additional_properties = d
        return match_query

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
