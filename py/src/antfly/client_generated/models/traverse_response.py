from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.traversal_result import TraversalResult


T = TypeVar("T", bound="TraverseResponse")


@_attrs_define
class TraverseResponse:
    """
    Attributes:
        results (Union[Unset, list['TraversalResult']]):
        count (Union[Unset, int]): Total number of results
    """

    results: Union[Unset, list["TraversalResult"]] = UNSET
    count: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        results: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.results, Unset):
            results = []
            for results_item_data in self.results:
                results_item = results_item_data.to_dict()
                results.append(results_item)

        count = self.count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if results is not UNSET:
            field_dict["results"] = results
        if count is not UNSET:
            field_dict["count"] = count

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.traversal_result import TraversalResult

        d = dict(src_dict)
        results = []
        _results = d.pop("results", UNSET)
        for results_item_data in _results or []:
            results_item = TraversalResult.from_dict(results_item_data)

            results.append(results_item)

        count = d.pop("count", UNSET)

        traverse_response = cls(
            results=results,
            count=count,
        )

        traverse_response.additional_properties = d
        return traverse_response

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
