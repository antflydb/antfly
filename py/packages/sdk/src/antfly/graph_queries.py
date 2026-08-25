"""Ergonomic, validated constructors for canonical graph queries."""

from typing import TypeAlias

from .client_generated.models.graph_alias_count_aggregate import GraphAliasCountAggregate
from .client_generated.models.graph_row_count_aggregate import GraphRowCountAggregate
from .client_generated.models.graph_row_count_target import GraphRowCountTarget
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
