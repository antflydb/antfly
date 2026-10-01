from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.extraction_attribute_label import ExtractionAttributeLabel


T = TypeVar("T", bound="ExtractionEntityAttributes")


@_attrs_define
class ExtractionEntityAttributes:
    """Version 2 span attributes. Attribute confidence is retained independently of include_confidence."""

    additional_properties: dict[str, ExtractionAttributeLabel | list[ExtractionAttributeLabel]] = _attrs_field(
        init=False, factory=dict
    )

    def to_dict(self) -> dict[str, Any]:
        from ..models.extraction_attribute_label import ExtractionAttributeLabel

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, ExtractionAttributeLabel):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = []
                for componentsschemas_extraction_attribute_selection_type_1_item_data in prop:
                    componentsschemas_extraction_attribute_selection_type_1_item = (
                        componentsschemas_extraction_attribute_selection_type_1_item_data.to_dict()
                    )
                    field_dict[prop_name].append(componentsschemas_extraction_attribute_selection_type_1_item)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_attribute_label import ExtractionAttributeLabel

        d = dict(src_dict)
        extraction_entity_attributes = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(data: object) -> ExtractionAttributeLabel | list[ExtractionAttributeLabel]:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_attribute_selection_type_0 = ExtractionAttributeLabel.from_dict(data)

                    return componentsschemas_extraction_attribute_selection_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, list):
                    raise TypeError()
                componentsschemas_extraction_attribute_selection_type_1 = []
                _componentsschemas_extraction_attribute_selection_type_1 = data
                for (
                    componentsschemas_extraction_attribute_selection_type_1_item_data
                ) in _componentsschemas_extraction_attribute_selection_type_1:
                    componentsschemas_extraction_attribute_selection_type_1_item = ExtractionAttributeLabel.from_dict(
                        componentsschemas_extraction_attribute_selection_type_1_item_data
                    )

                    componentsschemas_extraction_attribute_selection_type_1.append(
                        componentsschemas_extraction_attribute_selection_type_1_item
                    )

                return componentsschemas_extraction_attribute_selection_type_1

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        extraction_entity_attributes.additional_properties = additional_properties
        return extraction_entity_attributes

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> ExtractionAttributeLabel | list[ExtractionAttributeLabel]:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: ExtractionAttributeLabel | list[ExtractionAttributeLabel]) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
