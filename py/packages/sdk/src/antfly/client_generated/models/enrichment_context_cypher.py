from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_context_cypher_parameters import EnrichmentContextCypherParameters


T = TypeVar("T", bound="EnrichmentContextCypher")


@_attrs_define
class EnrichmentContextCypher:
    """Ranking clause: a read-only Cypher statement against a graph, run as the principal that declared the enrichment (its
    graph and table permissions and row policies apply; the server binds the declaration to that principal, and revoking
    the credential stops rebuilds). Members are the nodes of the source `table` the statement returns, anywhere in a row
    (bare nodes, paths, lists, maps), in result order, each once, up to `limit`, and render as those rows' `fields`.
    Nodes of other tables are context the statement used, not members. Only `limit` may sit beside it. A Cypher source
    is a snapshot: a statement's result depends on rows it does not return, so dependents rebuild on `refresh_interval`
    or manually. Statements that write (`CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE`) or `EXPLAIN` are rejected.

        Attributes:
            graph (str): Graph name (see `/graphs/{graphName}`).
            query (str): Cypher statement.
            parameters (EnrichmentContextCypherParameters | Unset): Statement parameters; values are literals or
                `{"$source":"/pointer"}` bindings from the dependent row.
    """

    graph: str
    query: str
    parameters: EnrichmentContextCypherParameters | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        graph = self.graph

        query = self.query

        parameters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = self.parameters.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "graph": graph,
                "query": query,
            }
        )
        if parameters is not UNSET:
            field_dict["parameters"] = parameters

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_context_cypher_parameters import EnrichmentContextCypherParameters

        d = dict(src_dict)
        graph = d.pop("graph")

        query = d.pop("query")

        _parameters = d.pop("parameters", UNSET)
        parameters: EnrichmentContextCypherParameters | Unset
        if isinstance(_parameters, Unset):
            parameters = UNSET
        else:
            parameters = EnrichmentContextCypherParameters.from_dict(_parameters)

        enrichment_context_cypher = cls(
            graph=graph,
            query=query,
            parameters=parameters,
        )

        return enrichment_context_cypher
