from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.extraction_joint_entity import ExtractionJointEntity


T = TypeVar("T", bound="ExtractionJointSchemaEntities")


@_attrs_define
class ExtractionJointSchemaEntities:
    """ """

    additional_properties: dict[str, ExtractionJointEntity | str] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.extraction_joint_entity import ExtractionJointEntity

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, ExtractionJointEntity):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_joint_entity import ExtractionJointEntity

        d = dict(src_dict)
        extraction_joint_schema_entities = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(data: object) -> ExtractionJointEntity | str:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    additional_property_type_1 = ExtractionJointEntity.from_dict(data)

                    return additional_property_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                return cast(ExtractionJointEntity | str, data)

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        extraction_joint_schema_entities.additional_properties = additional_properties
        return extraction_joint_schema_entities

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> ExtractionJointEntity | str:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: ExtractionJointEntity | str) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
