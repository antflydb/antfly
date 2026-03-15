from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.geo_point import GeoPoint


T = TypeVar("T", bound="GeoBoundingPolygonQuery")


@_attrs_define
class GeoBoundingPolygonQuery:
    """
    Attributes:
        polygon_points (list['GeoPoint']):
        field (Union[Unset, str]):
        boost (Union[None, Unset, float]): A floating-point number used to decrease or increase the relevance scores of
            a query.
    """

    polygon_points: list["GeoPoint"]
    field: Union[Unset, str] = UNSET
    boost: Union[None, Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        polygon_points = []
        for polygon_points_item_data in self.polygon_points:
            polygon_points_item = polygon_points_item_data.to_dict()
            polygon_points.append(polygon_points_item)

        field = self.field

        boost: Union[None, Unset, float]
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "polygon_points": polygon_points,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.geo_point import GeoPoint

        d = dict(src_dict)
        polygon_points = []
        _polygon_points = d.pop("polygon_points")
        for polygon_points_item_data in _polygon_points:
            polygon_points_item = GeoPoint.from_dict(polygon_points_item_data)

            polygon_points.append(polygon_points_item)

        field = d.pop("field", UNSET)

        def _parse_boost(data: object) -> Union[None, Unset, float]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, float], data)

        boost = _parse_boost(d.pop("boost", UNSET))

        geo_bounding_polygon_query = cls(
            polygon_points=polygon_points,
            field=field,
            boost=boost,
        )

        geo_bounding_polygon_query.additional_properties = d
        return geo_bounding_polygon_query

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
