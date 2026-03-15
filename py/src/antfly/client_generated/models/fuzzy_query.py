from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.fuzziness_type_1 import FuzzinessType1
from ..types import UNSET, Unset

T = TypeVar("T", bound="FuzzyQuery")


@_attrs_define
class FuzzyQuery:
    """
    Attributes:
        term (str):
        prefix_length (Union[Unset, int]):
        fuzziness (Union[FuzzinessType1, Unset, int]): The fuzziness of the query. Can be an integer or "auto".
        field (Union[Unset, str]):
        boost (Union[None, Unset, float]): A floating-point number used to decrease or increase the relevance scores of
            a query.
    """

    term: str
    prefix_length: Union[Unset, int] = UNSET
    fuzziness: Union[FuzzinessType1, Unset, int] = UNSET
    field: Union[Unset, str] = UNSET
    boost: Union[None, Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        term = self.term

        prefix_length = self.prefix_length

        fuzziness: Union[Unset, int, str]
        if isinstance(self.fuzziness, Unset):
            fuzziness = UNSET
        elif isinstance(self.fuzziness, FuzzinessType1):
            fuzziness = self.fuzziness.value
        else:
            fuzziness = self.fuzziness

        field = self.field

        boost: Union[None, Unset, float]
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "term": term,
            }
        )
        if prefix_length is not UNSET:
            field_dict["prefix_length"] = prefix_length
        if fuzziness is not UNSET:
            field_dict["fuzziness"] = fuzziness
        if field is not UNSET:
            field_dict["field"] = field
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        term = d.pop("term")

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

        field = d.pop("field", UNSET)

        def _parse_boost(data: object) -> Union[None, Unset, float]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, float], data)

        boost = _parse_boost(d.pop("boost", UNSET))

        fuzzy_query = cls(
            term=term,
            prefix_length=prefix_length,
            fuzziness=fuzziness,
            field=field,
            boost=boost,
        )

        fuzzy_query.additional_properties = d
        return fuzzy_query

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
