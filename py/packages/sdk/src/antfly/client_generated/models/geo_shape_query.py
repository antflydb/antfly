from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.geo_shape_geometry import GeoShapeGeometry


T = TypeVar("T", bound="GeoShapeQuery")


@_attrs_define
class GeoShapeQuery:
    """
    Attributes:
        geometry (GeoShapeGeometry):
        field (str | Unset):
        boost (float | None | Unset): A floating-point number used to decrease or increase the relevance scores of a
            query.
    """

    geometry: GeoShapeGeometry
    field: str | Unset = UNSET
    boost: float | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        geometry = self.geometry.to_dict()

        field = self.field

        boost: float | None | Unset
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "geometry": geometry,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.geo_shape_geometry import GeoShapeGeometry

        d = dict(src_dict)
        geometry = GeoShapeGeometry.from_dict(d.pop("geometry"))

        field = d.pop("field", UNSET)

        def _parse_boost(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        boost = _parse_boost(d.pop("boost", UNSET))

        geo_shape_query = cls(
            geometry=geometry,
            field=field,
            boost=boost,
        )

        geo_shape_query.additional_properties = d
        return geo_shape_query

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
