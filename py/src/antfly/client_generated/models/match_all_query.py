from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.match_all_query_match_all import MatchAllQueryMatchAll


T = TypeVar("T", bound="MatchAllQuery")


@_attrs_define
class MatchAllQuery:
    """
    Attributes:
        match_all (MatchAllQueryMatchAll):
        boost (Union[None, Unset, float]): A floating-point number used to decrease or increase the relevance scores of
            a query.
    """

    match_all: "MatchAllQueryMatchAll"
    boost: Union[None, Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        match_all = self.match_all.to_dict()

        boost: Union[None, Unset, float]
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "match_all": match_all,
            }
        )
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.match_all_query_match_all import MatchAllQueryMatchAll

        d = dict(src_dict)
        match_all = MatchAllQueryMatchAll.from_dict(d.pop("match_all"))

        def _parse_boost(data: object) -> Union[None, Unset, float]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, float], data)

        boost = _parse_boost(d.pop("boost", UNSET))

        match_all_query = cls(
            match_all=match_all,
            boost=boost,
        )

        match_all_query.additional_properties = d
        return match_all_query

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
