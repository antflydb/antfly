"""Ergonomic, validated constructors for canonical graph queries."""

from datetime import datetime
from math import isfinite
from typing import TypeAlias

from .client_generated.models.graph_alias_count_aggregate import GraphAliasCountAggregate
from .client_generated.models.graph_document_date_range_body import GraphDocumentDateRangeBody
from .client_generated.models.graph_document_date_range_filter import GraphDocumentDateRangeFilter
from .client_generated.models.graph_document_numeric_range_body import GraphDocumentNumericRangeBody
from .client_generated.models.graph_document_numeric_range_filter import GraphDocumentNumericRangeFilter
from .client_generated.models.graph_document_term_range_body import GraphDocumentTermRangeBody
from .client_generated.models.graph_document_term_range_filter import GraphDocumentTermRangeFilter
from .client_generated.models.graph_row_count_aggregate import GraphRowCountAggregate
from .client_generated.models.graph_row_count_target import GraphRowCountTarget
from .client_generated.types import UNSET
from .exceptions import AntflyException
from .graph_identifier_policy_generated import is_valid_graph_identifier

GraphCountAggregate: TypeAlias = GraphRowCountAggregate | GraphAliasCountAggregate


def require_graph_identifier(value: object, path: str) -> None:
    """Raise a stable SDK error when a public graph identifier is invalid."""
    if not isinstance(value, str) or not is_valid_graph_identifier(value):
        raise AntflyException(
            f"{path} must satisfy the versioned GraphIdentifier policy "
            "(1-128 Unicode code points; no reserved, boundary-space, non-ASCII whitespace, "
            "control, or format characters)"
        )


def count_graph_rows() -> GraphRowCountAggregate:
    """Construct an exact count over complete graph bindings."""
    return GraphRowCountAggregate(count=GraphRowCountTarget.VALUE_0)


def count_graph_alias(alias: str, distinct: bool = False) -> GraphAliasCountAggregate:
    """Construct an exact count over the non-null bindings of one alias."""
    require_graph_identifier(alias, "graph count alias")
    return GraphAliasCountAggregate(count=alias, distinct=distinct)


def _require_graph_document_path(path: str) -> None:
    if not path.startswith("/"):
        raise AntflyException("graph document filter path must be a valid RFC 6901 JSON Pointer")
    index = 0
    while index < len(path):
        if path[index] == "~":
            if index + 1 >= len(path) or path[index + 1] not in "01":
                raise AntflyException("graph document filter path must be a valid RFC 6901 JSON Pointer")
            index += 1
        index += 1


def graph_numeric_range_filter(
    path: str,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
    inclusive_min: bool | None = None,
    inclusive_max: bool | None = None,
) -> GraphDocumentNumericRangeFilter:
    """Construct a validated non-scoring numeric range predicate for a graph node."""
    _require_graph_document_path(path)
    if min_value is None and max_value is None:
        raise AntflyException("graph numeric range requires min_value or max_value")
    if (min_value is not None and not isfinite(min_value)) or (max_value is not None and not isfinite(max_value)):
        raise AntflyException("graph numeric range bounds must be finite numbers")
    return GraphDocumentNumericRangeFilter(
        numeric_range=GraphDocumentNumericRangeBody(
            path=path,
            min_=min_value if min_value is not None else UNSET,
            max_=max_value if max_value is not None else UNSET,
            inclusive_min=inclusive_min if inclusive_min is not None else UNSET,
            inclusive_max=inclusive_max if inclusive_max is not None else UNSET,
        )
    )


def graph_term_range_filter(
    path: str,
    *,
    min_value: str | None = None,
    max_value: str | None = None,
    inclusive_min: bool | None = None,
    inclusive_max: bool | None = None,
) -> GraphDocumentTermRangeFilter:
    """Construct a validated non-scoring lexical range predicate for a graph node."""
    _require_graph_document_path(path)
    if min_value is None and max_value is None:
        raise AntflyException("graph term range requires min_value or max_value")
    return GraphDocumentTermRangeFilter(
        term_range=GraphDocumentTermRangeBody(
            path=path,
            min_=min_value if min_value is not None else UNSET,
            max_=max_value if max_value is not None else UNSET,
            inclusive_min=inclusive_min if inclusive_min is not None else UNSET,
            inclusive_max=inclusive_max if inclusive_max is not None else UNSET,
        )
    )


def graph_date_range_filter(
    path: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    inclusive_start: bool | None = None,
    inclusive_end: bool | None = None,
) -> GraphDocumentDateRangeFilter:
    """Construct a validated non-scoring date range predicate for a graph node."""
    _require_graph_document_path(path)
    if start is None and end is None:
        raise AntflyException("graph date range requires start or end")
    if (start is not None and start.utcoffset() is None) or (end is not None and end.utcoffset() is None):
        raise AntflyException("graph date range bounds must include a UTC offset")
    return GraphDocumentDateRangeFilter(
        date_range=GraphDocumentDateRangeBody(
            path=path,
            start=start if start is not None else UNSET,
            end=end if end is not None else UNSET,
            inclusive_start=inclusive_start if inclusive_start is not None else UNSET,
            inclusive_end=inclusive_end if inclusive_end is not None else UNSET,
        )
    )
