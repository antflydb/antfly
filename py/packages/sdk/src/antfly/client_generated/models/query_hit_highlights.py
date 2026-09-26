from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.highlight_fragment import HighlightFragment


T = TypeVar("T", bound="QueryHitHighlights")


@_attrs_define
class QueryHitHighlights:
    """Highlighted fragments keyed by source field, present when the request
    set `highlight` and the stored document has the field. Highlights are
    computed from the unprojected document, so a `fields` projection that
    omits a highlighted field does not suppress its highlights. Each
    fragment is a window of the stored field value with byte-offset spans
    marking the text the full-text query matched.

    """

    additional_properties: dict[str, list[HighlightFragment]] = _attrs_field(init=False, factory=dict)

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
        from ..models.highlight_fragment import HighlightFragment

        d = dict(src_dict)
        query_hit_highlights = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = []
            _additional_property = prop_dict
            for additional_property_item_data in _additional_property:
                additional_property_item = HighlightFragment.from_dict(additional_property_item_data)

                additional_property.append(additional_property_item)

            additional_properties[prop_name] = additional_property

        query_hit_highlights.additional_properties = additional_properties
        return query_hit_highlights

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> list[HighlightFragment]:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: list[HighlightFragment]) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
