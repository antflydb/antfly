from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.embeddings_index_stats_index_type import EmbeddingsIndexStatsIndexType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.embeddings_index_stats_async_indexing import EmbeddingsIndexStatsAsyncIndexing
    from ..models.embeddings_index_stats_enrichment_runtime import EmbeddingsIndexStatsEnrichmentRuntime
    from ..models.embeddings_index_stats_hbc_cache import EmbeddingsIndexStatsHbcCache
    from ..models.embeddings_index_stats_hbc_posting import EmbeddingsIndexStatsHbcPosting


T = TypeVar("T", bound="EmbeddingsIndexStats")


@_attrs_define
class EmbeddingsIndexStats:
    """Statistics for an embeddings index (dense or sparse)

    Attributes:
        index_type (EmbeddingsIndexStatsIndexType): Discriminator for the index stats variant.
        error (str | Unset): Error message if stats could not be retrieved
        total_indexed (int | Unset): Number of vectors/documents in the index
        total_nodes (int | Unset): Total number of nodes in the index (dense only)
        total_terms (int | Unset): Number of unique terms in the inverted index (sparse only)
        rebuilding (bool | Unset): Whether the index enricher is currently backfilling
        backfill_active (bool | Unset): Whether the index is actively rebuilding, replaying, enriching, or catching up.
        backfill_progress (float | Unset): Backfill progress as a ratio from 0.0 to 1.0
        backfill_state (str | Unset): Operational readiness state such as ready, running, retrying, or failed.
        doc_count (int | Unset): Number of documents visible to the index.
        query_visible_doc_count (int | Unset): Documents currently visible to queries.
        published_doc_count (int | Unset):
        published_node_count (int | Unset):
        root_node (int | Unset):
        published_root_node (int | Unset):
        dense_replay_applied_sequence (int | Unset):
        dense_replay_target_sequence (int | Unset):
        dense_publish_pending (bool | Unset): Whether dense/vector artifacts still need publication before queries see
            the latest data.
        replay_applied_sequence (int | Unset):
        replay_target_sequence (int | Unset):
        replay_catch_up_required (bool | Unset):
        runtime_present (bool | Unset):
        runtime_fresh (bool | Unset):
        runtime_source (str | Unset):
        runtime_freshness (str | Unset):
        catch_up_active (bool | Unset):
        catch_up_phase (str | Unset):
        catch_up_applied_sequence (int | Unset):
        catch_up_target_sequence (int | Unset):
        enrichment_runtime (EmbeddingsIndexStatsEnrichmentRuntime | Unset): Embedding enrichment worker runtime
            diagnostics.
        hbc_cache (EmbeddingsIndexStatsHbcCache | Unset):
        hbc_posting (EmbeddingsIndexStatsHbcPosting | Unset):
        async_indexing (EmbeddingsIndexStatsAsyncIndexing | Unset):
    """

    index_type: EmbeddingsIndexStatsIndexType
    error: str | Unset = UNSET
    total_indexed: int | Unset = UNSET
    total_nodes: int | Unset = UNSET
    total_terms: int | Unset = UNSET
    rebuilding: bool | Unset = UNSET
    backfill_active: bool | Unset = UNSET
    backfill_progress: float | Unset = UNSET
    backfill_state: str | Unset = UNSET
    doc_count: int | Unset = UNSET
    query_visible_doc_count: int | Unset = UNSET
    published_doc_count: int | Unset = UNSET
    published_node_count: int | Unset = UNSET
    root_node: int | Unset = UNSET
    published_root_node: int | Unset = UNSET
    dense_replay_applied_sequence: int | Unset = UNSET
    dense_replay_target_sequence: int | Unset = UNSET
    dense_publish_pending: bool | Unset = UNSET
    replay_applied_sequence: int | Unset = UNSET
    replay_target_sequence: int | Unset = UNSET
    replay_catch_up_required: bool | Unset = UNSET
    runtime_present: bool | Unset = UNSET
    runtime_fresh: bool | Unset = UNSET
    runtime_source: str | Unset = UNSET
    runtime_freshness: str | Unset = UNSET
    catch_up_active: bool | Unset = UNSET
    catch_up_phase: str | Unset = UNSET
    catch_up_applied_sequence: int | Unset = UNSET
    catch_up_target_sequence: int | Unset = UNSET
    enrichment_runtime: EmbeddingsIndexStatsEnrichmentRuntime | Unset = UNSET
    hbc_cache: EmbeddingsIndexStatsHbcCache | Unset = UNSET
    hbc_posting: EmbeddingsIndexStatsHbcPosting | Unset = UNSET
    async_indexing: EmbeddingsIndexStatsAsyncIndexing | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        error = self.error

        total_indexed = self.total_indexed

        total_nodes = self.total_nodes

        total_terms = self.total_terms

        rebuilding = self.rebuilding

        backfill_active = self.backfill_active

        backfill_progress = self.backfill_progress

        backfill_state = self.backfill_state

        doc_count = self.doc_count

        query_visible_doc_count = self.query_visible_doc_count

        published_doc_count = self.published_doc_count

        published_node_count = self.published_node_count

        root_node = self.root_node

        published_root_node = self.published_root_node

        dense_replay_applied_sequence = self.dense_replay_applied_sequence

        dense_replay_target_sequence = self.dense_replay_target_sequence

        dense_publish_pending = self.dense_publish_pending

        replay_applied_sequence = self.replay_applied_sequence

        replay_target_sequence = self.replay_target_sequence

        replay_catch_up_required = self.replay_catch_up_required

        runtime_present = self.runtime_present

        runtime_fresh = self.runtime_fresh

        runtime_source = self.runtime_source

        runtime_freshness = self.runtime_freshness

        catch_up_active = self.catch_up_active

        catch_up_phase = self.catch_up_phase

        catch_up_applied_sequence = self.catch_up_applied_sequence

        catch_up_target_sequence = self.catch_up_target_sequence

        enrichment_runtime: dict[str, Any] | Unset = UNSET
        if not isinstance(self.enrichment_runtime, Unset):
            enrichment_runtime = self.enrichment_runtime.to_dict()

        hbc_cache: dict[str, Any] | Unset = UNSET
        if not isinstance(self.hbc_cache, Unset):
            hbc_cache = self.hbc_cache.to_dict()

        hbc_posting: dict[str, Any] | Unset = UNSET
        if not isinstance(self.hbc_posting, Unset):
            hbc_posting = self.hbc_posting.to_dict()

        async_indexing: dict[str, Any] | Unset = UNSET
        if not isinstance(self.async_indexing, Unset):
            async_indexing = self.async_indexing.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index_type": index_type,
            }
        )
        if error is not UNSET:
            field_dict["error"] = error
        if total_indexed is not UNSET:
            field_dict["total_indexed"] = total_indexed
        if total_nodes is not UNSET:
            field_dict["total_nodes"] = total_nodes
        if total_terms is not UNSET:
            field_dict["total_terms"] = total_terms
        if rebuilding is not UNSET:
            field_dict["rebuilding"] = rebuilding
        if backfill_active is not UNSET:
            field_dict["backfill_active"] = backfill_active
        if backfill_progress is not UNSET:
            field_dict["backfill_progress"] = backfill_progress
        if backfill_state is not UNSET:
            field_dict["backfill_state"] = backfill_state
        if doc_count is not UNSET:
            field_dict["doc_count"] = doc_count
        if query_visible_doc_count is not UNSET:
            field_dict["query_visible_doc_count"] = query_visible_doc_count
        if published_doc_count is not UNSET:
            field_dict["published_doc_count"] = published_doc_count
        if published_node_count is not UNSET:
            field_dict["published_node_count"] = published_node_count
        if root_node is not UNSET:
            field_dict["root_node"] = root_node
        if published_root_node is not UNSET:
            field_dict["published_root_node"] = published_root_node
        if dense_replay_applied_sequence is not UNSET:
            field_dict["dense_replay_applied_sequence"] = dense_replay_applied_sequence
        if dense_replay_target_sequence is not UNSET:
            field_dict["dense_replay_target_sequence"] = dense_replay_target_sequence
        if dense_publish_pending is not UNSET:
            field_dict["dense_publish_pending"] = dense_publish_pending
        if replay_applied_sequence is not UNSET:
            field_dict["replay_applied_sequence"] = replay_applied_sequence
        if replay_target_sequence is not UNSET:
            field_dict["replay_target_sequence"] = replay_target_sequence
        if replay_catch_up_required is not UNSET:
            field_dict["replay_catch_up_required"] = replay_catch_up_required
        if runtime_present is not UNSET:
            field_dict["runtime_present"] = runtime_present
        if runtime_fresh is not UNSET:
            field_dict["runtime_fresh"] = runtime_fresh
        if runtime_source is not UNSET:
            field_dict["runtime_source"] = runtime_source
        if runtime_freshness is not UNSET:
            field_dict["runtime_freshness"] = runtime_freshness
        if catch_up_active is not UNSET:
            field_dict["catch_up_active"] = catch_up_active
        if catch_up_phase is not UNSET:
            field_dict["catch_up_phase"] = catch_up_phase
        if catch_up_applied_sequence is not UNSET:
            field_dict["catch_up_applied_sequence"] = catch_up_applied_sequence
        if catch_up_target_sequence is not UNSET:
            field_dict["catch_up_target_sequence"] = catch_up_target_sequence
        if enrichment_runtime is not UNSET:
            field_dict["enrichment_runtime"] = enrichment_runtime
        if hbc_cache is not UNSET:
            field_dict["hbc_cache"] = hbc_cache
        if hbc_posting is not UNSET:
            field_dict["hbc_posting"] = hbc_posting
        if async_indexing is not UNSET:
            field_dict["async_indexing"] = async_indexing

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.embeddings_index_stats_async_indexing import EmbeddingsIndexStatsAsyncIndexing
        from ..models.embeddings_index_stats_enrichment_runtime import EmbeddingsIndexStatsEnrichmentRuntime
        from ..models.embeddings_index_stats_hbc_cache import EmbeddingsIndexStatsHbcCache
        from ..models.embeddings_index_stats_hbc_posting import EmbeddingsIndexStatsHbcPosting

        d = dict(src_dict)
        index_type = EmbeddingsIndexStatsIndexType(d.pop("index_type"))

        error = d.pop("error", UNSET)

        total_indexed = d.pop("total_indexed", UNSET)

        total_nodes = d.pop("total_nodes", UNSET)

        total_terms = d.pop("total_terms", UNSET)

        rebuilding = d.pop("rebuilding", UNSET)

        backfill_active = d.pop("backfill_active", UNSET)

        backfill_progress = d.pop("backfill_progress", UNSET)

        backfill_state = d.pop("backfill_state", UNSET)

        doc_count = d.pop("doc_count", UNSET)

        query_visible_doc_count = d.pop("query_visible_doc_count", UNSET)

        published_doc_count = d.pop("published_doc_count", UNSET)

        published_node_count = d.pop("published_node_count", UNSET)

        root_node = d.pop("root_node", UNSET)

        published_root_node = d.pop("published_root_node", UNSET)

        dense_replay_applied_sequence = d.pop("dense_replay_applied_sequence", UNSET)

        dense_replay_target_sequence = d.pop("dense_replay_target_sequence", UNSET)

        dense_publish_pending = d.pop("dense_publish_pending", UNSET)

        replay_applied_sequence = d.pop("replay_applied_sequence", UNSET)

        replay_target_sequence = d.pop("replay_target_sequence", UNSET)

        replay_catch_up_required = d.pop("replay_catch_up_required", UNSET)

        runtime_present = d.pop("runtime_present", UNSET)

        runtime_fresh = d.pop("runtime_fresh", UNSET)

        runtime_source = d.pop("runtime_source", UNSET)

        runtime_freshness = d.pop("runtime_freshness", UNSET)

        catch_up_active = d.pop("catch_up_active", UNSET)

        catch_up_phase = d.pop("catch_up_phase", UNSET)

        catch_up_applied_sequence = d.pop("catch_up_applied_sequence", UNSET)

        catch_up_target_sequence = d.pop("catch_up_target_sequence", UNSET)

        _enrichment_runtime = d.pop("enrichment_runtime", UNSET)
        enrichment_runtime: EmbeddingsIndexStatsEnrichmentRuntime | Unset
        if isinstance(_enrichment_runtime, Unset):
            enrichment_runtime = UNSET
        else:
            enrichment_runtime = EmbeddingsIndexStatsEnrichmentRuntime.from_dict(_enrichment_runtime)

        _hbc_cache = d.pop("hbc_cache", UNSET)
        hbc_cache: EmbeddingsIndexStatsHbcCache | Unset
        if isinstance(_hbc_cache, Unset):
            hbc_cache = UNSET
        else:
            hbc_cache = EmbeddingsIndexStatsHbcCache.from_dict(_hbc_cache)

        _hbc_posting = d.pop("hbc_posting", UNSET)
        hbc_posting: EmbeddingsIndexStatsHbcPosting | Unset
        if isinstance(_hbc_posting, Unset):
            hbc_posting = UNSET
        else:
            hbc_posting = EmbeddingsIndexStatsHbcPosting.from_dict(_hbc_posting)

        _async_indexing = d.pop("async_indexing", UNSET)
        async_indexing: EmbeddingsIndexStatsAsyncIndexing | Unset
        if isinstance(_async_indexing, Unset):
            async_indexing = UNSET
        else:
            async_indexing = EmbeddingsIndexStatsAsyncIndexing.from_dict(_async_indexing)

        embeddings_index_stats = cls(
            index_type=index_type,
            error=error,
            total_indexed=total_indexed,
            total_nodes=total_nodes,
            total_terms=total_terms,
            rebuilding=rebuilding,
            backfill_active=backfill_active,
            backfill_progress=backfill_progress,
            backfill_state=backfill_state,
            doc_count=doc_count,
            query_visible_doc_count=query_visible_doc_count,
            published_doc_count=published_doc_count,
            published_node_count=published_node_count,
            root_node=root_node,
            published_root_node=published_root_node,
            dense_replay_applied_sequence=dense_replay_applied_sequence,
            dense_replay_target_sequence=dense_replay_target_sequence,
            dense_publish_pending=dense_publish_pending,
            replay_applied_sequence=replay_applied_sequence,
            replay_target_sequence=replay_target_sequence,
            replay_catch_up_required=replay_catch_up_required,
            runtime_present=runtime_present,
            runtime_fresh=runtime_fresh,
            runtime_source=runtime_source,
            runtime_freshness=runtime_freshness,
            catch_up_active=catch_up_active,
            catch_up_phase=catch_up_phase,
            catch_up_applied_sequence=catch_up_applied_sequence,
            catch_up_target_sequence=catch_up_target_sequence,
            enrichment_runtime=enrichment_runtime,
            hbc_cache=hbc_cache,
            hbc_posting=hbc_posting,
            async_indexing=async_indexing,
        )

        embeddings_index_stats.additional_properties = d
        return embeddings_index_stats

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
