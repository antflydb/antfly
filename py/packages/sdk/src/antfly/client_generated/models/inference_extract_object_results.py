from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.inference_extract_object_results_additional_property_item import (
        InferenceExtractObjectResultsAdditionalPropertyItem,
    )


T = TypeVar("T", bound="InferenceExtractObjectResults")


@_attrs_define
class InferenceExtractObjectResults:
    """Extraction result for this input. Maps structure names to arrays of extracted instances.
    Each instance maps field names to ExtractFieldValue (for ::str fields)
    or arrays of ExtractFieldValue (for ::list fields).

    """

    additional_properties: dict[str, list[InferenceExtractObjectResultsAdditionalPropertyItem]] = _attrs_field(
        init=False, factory=dict
    )

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = []
            for additional_property_item_data in prop:
                additional_property_item = additional_property_item_data.to_dict()
                field_dict[prop_name].append(additional_property_item)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_extract_object_results_additional_property_item import (
            InferenceExtractObjectResultsAdditionalPropertyItem,
        )

        d = dict(src_dict)
        inference_extract_object_results = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = []
            _additional_property = prop_dict
            for additional_property_item_data in _additional_property:
                additional_property_item = InferenceExtractObjectResultsAdditionalPropertyItem.from_dict(
                    additional_property_item_data
                )

                additional_property.append(additional_property_item)

            additional_properties[prop_name] = additional_property

        inference_extract_object_results.additional_properties = additional_properties
        return inference_extract_object_results

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> list[InferenceExtractObjectResultsAdditionalPropertyItem]:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: list[InferenceExtractObjectResultsAdditionalPropertyItem]) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
