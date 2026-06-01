from enum import Enum


class GeoShapeGeometryRelation(str, Enum):
    CONTAINS = "contains"
    INTERSECTS = "intersects"
    WITHIN = "within"

    def __str__(self) -> str:
        return str(self.value)
