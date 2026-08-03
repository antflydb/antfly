from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.create_table_request_indexes import CreateTableRequestIndexes
    from ..models.replication_source import ReplicationSource
    from ..models.table_schema import TableSchema


T = TypeVar("T", bound="CreateTableRequest")


@_attrs_define
class CreateTableRequest:
    """
    Attributes:
        num_shards (int | Unset): Number of shards to create for the table. Data is partitioned across shards based on
            key ranges.

            **Sizing Guidelines:**
            - Small datasets (<100K docs): 1-3 shards
            - Medium datasets (100K-1M docs): 3-10 shards
            - Large datasets (>1M docs): 10+ shards

            More shards enable better parallelism but increase overhead. Choose based on expected data size and query
            patterns.

            **When to Add More Shards:**

            Antfly supports **online shard reallocation** without downtime. Add more shards when:
            - Individual shards exceed size thresholds (configurable)
            - Query latency increases due to large shard size
            - Need better parallelism for write-heavy workloads

            Use the internal `/reallocate` endpoint to trigger automatic shard splitting:
            ```bash
            POST /internal/v1/reallocate
            ```

            This enqueues a reallocation request that the leader processes asynchronously, splitting
            large shards and redistributing data without service interruption.

            **Advantages over Elasticsearch:**
            - Automatic shard splitting (no manual reindexing required)
            - Online operation (no downtime)
            - Transparent to applications (keys remain accessible during reallocation)
             Example: 3.
        description (str | Unset): Optional human-readable description of the table and its purpose.
            Useful for documentation and team collaboration.
             Example: User profiles with embeddings for semantic search.
        indexes (CreateTableRequestIndexes | Unset): Map of index name to index configuration. Indexes enable different
            query capabilities:
            - Full-text indexes for BM25 search
            - Vector indexes for semantic similarity
            - Multimodal indexes for images/audio/video

            You can add multiple indexes to support different query patterns.
             Example: {'search_index': {'type': 'full_text'}, 'embedding_index': {'type': 'embeddings', 'dimension': 384,
            'embedder': {'provider': 'ollama', 'model': 'all-minilm'}}}.
        schema (TableSchema | Unset): Schema definition for a table with multiple document types
        replication_sources (list[ReplicationSource] | Unset): PostgreSQL CDC replication sources. Streams
            INSERT/UPDATE/DELETE changes from
            PostgreSQL tables into this Antfly table via logical replication.

            Multiple sources can feed into a single table (e.g., `users` + `scores` → Antfly `users`).
            Each source uses `on_update`/`on_delete` transforms to control how PG events map to
            Antfly document operations. Requires `wal_level=logical` on the PostgreSQL source.
    """

    num_shards: int | Unset = UNSET
    description: str | Unset = UNSET
    indexes: CreateTableRequestIndexes | Unset = UNSET
    schema: TableSchema | Unset = UNSET
    replication_sources: list[ReplicationSource] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        num_shards = self.num_shards

        description = self.description

        indexes: dict[str, Any] | Unset = UNSET
        if not isinstance(self.indexes, Unset):
            indexes = self.indexes.to_dict()

        schema: dict[str, Any] | Unset = UNSET
        if not isinstance(self.schema, Unset):
            schema = self.schema.to_dict()

        replication_sources: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.replication_sources, Unset):
            replication_sources = []
            for replication_sources_item_data in self.replication_sources:
                replication_sources_item = replication_sources_item_data.to_dict()
                replication_sources.append(replication_sources_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if num_shards is not UNSET:
            field_dict["num_shards"] = num_shards
        if description is not UNSET:
            field_dict["description"] = description
        if indexes is not UNSET:
            field_dict["indexes"] = indexes
        if schema is not UNSET:
            field_dict["schema"] = schema
        if replication_sources is not UNSET:
            field_dict["replication_sources"] = replication_sources

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.create_table_request_indexes import CreateTableRequestIndexes
        from ..models.replication_source import ReplicationSource
        from ..models.table_schema import TableSchema

        d = dict(src_dict)
        num_shards = d.pop("num_shards", UNSET)

        description = d.pop("description", UNSET)

        _indexes = d.pop("indexes", UNSET)
        indexes: CreateTableRequestIndexes | Unset
        if isinstance(_indexes, Unset):
            indexes = UNSET
        else:
            indexes = CreateTableRequestIndexes.from_dict(_indexes)

        _schema = d.pop("schema", UNSET)
        schema: TableSchema | Unset
        if isinstance(_schema, Unset):
            schema = UNSET
        else:
            schema = TableSchema.from_dict(_schema)

        _replication_sources = d.pop("replication_sources", UNSET)
        replication_sources: list[ReplicationSource] | Unset = UNSET
        if _replication_sources is not UNSET:
            replication_sources = []
            for replication_sources_item_data in _replication_sources:
                replication_sources_item = ReplicationSource.from_dict(replication_sources_item_data)

                replication_sources.append(replication_sources_item)

        create_table_request = cls(
            num_shards=num_shards,
            description=description,
            indexes=indexes,
            schema=schema,
            replication_sources=replication_sources,
        )

        create_table_request.additional_properties = d
        return create_table_request

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
