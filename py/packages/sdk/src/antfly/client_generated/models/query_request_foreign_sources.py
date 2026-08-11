from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.foreign_source import ForeignSource


T = TypeVar("T", bound="QueryRequestForeignSources")


@_attrs_define
class QueryRequestForeignSources:
    """Map of table name to foreign data source configuration for query-time federated access.
    When a table name referenced in this query (or in a join's `right_table`) appears as a key
    here, the query is routed to the external database instead of Antfly shards.

    This enables joining Antfly search results with structured relational data (customer records,
    product catalogs, etc.) without ingesting that data into Antfly.

    **Supported operations on foreign tables:** filter_query, field selection, limit/offset.
    **Not supported:** full_text_search, semantic_search, graph_searches, aggregations, reranker.

    **Example - Join Antfly products with Postgres customers:**
    ```json
    {
      "table": "products",
      "full_text_search": {"query": "category:electronics"},
      "join": {
        "right_table": "pg_customers",
        "on": {"left_field": "customer_id", "right_field": "id"}
      },
      "foreign_sources": {
        "pg_customers": {
          "type": "postgres",
          "dsn": "${secret:pg_dsn}",
          "postgres_table": "customers"
        }
      }
    }
    ```

    """

    additional_properties: dict[str, ForeignSource] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.foreign_source import ForeignSource

        d = dict(src_dict)
        query_request_foreign_sources = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = ForeignSource.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        query_request_foreign_sources.additional_properties = additional_properties
        return query_request_foreign_sources

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> ForeignSource:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: ForeignSource) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
