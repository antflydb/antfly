from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.geo_shape_geometry_relation import GeoShapeGeometryRelation

if TYPE_CHECKING:
    from ..models.geo_shape import GeoShape


T = TypeVar("T", bound="GeoShapeGeometry")


@_attrs_define
class GeoShapeGeometry:
    """
    Attributes:
        shape (GeoShape): A GeoJSON shape object. This is a simplified representation.
        relation (GeoShapeGeometryRelation):
    """

    shape: GeoShape
    relation: GeoShapeGeometryRelation
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        shape = self.shape.to_dict()

        relation = self.relation.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "shape": shape,
                "relation": relation,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.geo_shape import GeoShape

        d = dict(src_dict)
        shape = GeoShape.from_dict(d.pop("shape"))

        relation = GeoShapeGeometryRelation(d.pop("relation"))

        geo_shape_geometry = cls(
            shape=shape,
            relation=relation,
        )

        geo_shape_geometry.additional_properties = d
        return geo_shape_geometry

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
