from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="GeoBoundingBoxQuery")


@_attrs_define
class GeoBoundingBoxQuery:
    """Geographic bounding box filter. The public query shape uses scalar latitude and longitude bounds to match structured
    filter_query.geo_bbox. Longitude ranges may cross the antimeridian by specifying a western/min longitude that is
    greater than the eastern/max longitude; for example, min_lon 179.5 and max_lon -179.5 matches points near +/-180
    degrees longitude. Latitude bounds must be ordered with min_lat <= max_lat.

        Attributes:
            field (str): Field or path containing geo_point values.
            min_lat (float): Southern latitude bound.
            min_lon (float): Western longitude bound. If greater than max_lon, the box crosses the antimeridian.
            max_lat (float): Northern latitude bound. Must be greater than or equal to min_lat.
            max_lon (float): Eastern longitude bound. May be less than min_lon for antimeridian-wrapped boxes.
            boost (float | None | Unset): A floating-point number used to decrease or increase the relevance scores of a
                query.
    """

    field: str
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float
    boost: float | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        min_lat = self.min_lat

        min_lon = self.min_lon

        max_lat = self.max_lat

        max_lon = self.max_lon

        boost: float | None | Unset
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "field": field,
                "min_lat": min_lat,
                "min_lon": min_lon,
                "max_lat": max_lat,
                "max_lon": max_lon,
            }
        )
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        min_lat = d.pop("min_lat")

        min_lon = d.pop("min_lon")

        max_lat = d.pop("max_lat")

        max_lon = d.pop("max_lon")

        def _parse_boost(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        boost = _parse_boost(d.pop("boost", UNSET))

        geo_bounding_box_query = cls(
            field=field,
            min_lat=min_lat,
            min_lon=min_lon,
            max_lat=max_lat,
            max_lon=max_lon,
            boost=boost,
        )

        geo_bounding_box_query.additional_properties = d
        return geo_bounding_box_query

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
