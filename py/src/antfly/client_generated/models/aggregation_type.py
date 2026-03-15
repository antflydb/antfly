from enum import Enum


class AggregationType(str, Enum):
    AVG = "avg"
    CARDINALITY = "cardinality"
    COUNT = "count"
    DATE_HISTOGRAM = "date_histogram"
    DATE_RANGE = "date_range"
    GEOHASH_GRID = "geohash_grid"
    GEO_DISTANCE = "geo_distance"
    HISTOGRAM = "histogram"
    MAX = "max"
    MIN = "min"
    RANGE = "range"
    SIGNIFICANT_TERMS = "significant_terms"
    STATS = "stats"
    SUM = "sum"
    SUMSQUARES = "sumsquares"
    TERMS = "terms"

    def __str__(self) -> str:
        return str(self.value)
