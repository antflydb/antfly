from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="GroundTruth")


@_attrs_define
class GroundTruth:
    """Ground truth data for evaluation

    Attributes:
        relevant_ids (Union[Unset, list[str]]): Document IDs known to be relevant (for retrieval metrics)
        expectations (Union[Unset, str]): Context for evaluators about what to expect in the response.
            Provides guidance for LLM judges (e.g., "Should mention pricing tiers").
    """

    relevant_ids: Union[Unset, list[str]] = UNSET
    expectations: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        relevant_ids: Union[Unset, list[str]] = UNSET
        if not isinstance(self.relevant_ids, Unset):
            relevant_ids = self.relevant_ids

        expectations = self.expectations

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if relevant_ids is not UNSET:
            field_dict["relevant_ids"] = relevant_ids
        if expectations is not UNSET:
            field_dict["expectations"] = expectations

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        relevant_ids = cast(list[str], d.pop("relevant_ids", UNSET))

        expectations = d.pop("expectations", UNSET)

        ground_truth = cls(
            relevant_ids=relevant_ids,
            expectations=expectations,
        )

        ground_truth.additional_properties = d
        return ground_truth

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
