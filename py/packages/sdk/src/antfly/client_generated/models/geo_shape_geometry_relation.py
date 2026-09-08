from enum import StrEnum


class GeoShapeGeometryRelation(StrEnum):
    CONTAINS = "contains"
    INTERSECTS = "intersects"
    WITHIN = "within"

    def __str__(self) -> str:
        return str(self.value)
