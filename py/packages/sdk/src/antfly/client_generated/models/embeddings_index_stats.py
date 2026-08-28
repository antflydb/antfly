from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.dense_native_storage_phase import DenseNativeStoragePhase
from ..models.embeddings_index_stats_index_type import EmbeddingsIndexStatsIndexType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.derived_coverage_status import DerivedCoverageStatus
    from ..models.embeddings_index_stats_async_indexing import EmbeddingsIndexStatsAsyncIndexing
    from ..models.embeddings_index_stats_hbc_cache import EmbeddingsIndexStatsHbcCache
    from ..models.embeddings_index_stats_hbc_posting import EmbeddingsIndexStatsHbcPosting
    from ..models.embeddings_index_stats_promotion import EmbeddingsIndexStatsPromotion
    from ..models.embeddings_index_stats_resolution import EmbeddingsIndexStatsResolution
    from ..models.enrichment_runtime_status import EnrichmentRuntimeStatus
    from ..models.index_readiness_status import IndexReadinessStatus
    from ..models.index_repair_status import IndexRepairStatus


T = TypeVar("T", bound="EmbeddingsIndexStats")


@_attrs_define
class EmbeddingsIndexStats:
    """Statistics for an embeddings index (dense or sparse)

    Attributes:
        index_type (EmbeddingsIndexStatsIndexType): Discriminator for the index stats variant.
        readiness (IndexReadinessStatus | Unset):
        error (str | Unset): Error message if stats could not be retrieved
        total_indexed (int | Unset): Number of vectors/documents in the index
        disk_usage (int | Unset): Size of the index in bytes
        total_nodes (int | Unset): Total number of nodes in the index (dense only)
        total_terms (int | Unset): Number of unique terms in the inverted index (sparse only)
        rebuilding (bool | Unset): Whether enrichment, publication, or replay work is still pending. Documents that do
            not contain the indexed field are terminal skipped outcomes and do not keep this true.
        repair (IndexRepairStatus | Unset): Compact user-facing state for an automatic index repair. Detailed
            diagnostics are available from the admin API and metrics.
        wal_backlog (int | Unset): Number of documents pending enrichment in the WAL
        backfill_active (bool | Unset): Whether the index is actively rebuilding, replaying, enriching, or catching up.
        backfill_progress (float | Unset): Fraction of source documents with a terminal materialization outcome,
            including produced embeddings and intentionally skipped documents. Reaches 1.0 when no source work is pending
            and replay is current.
        backfill_items_processed (int | Unset): Total items processed during backfill
        backfill_state (str | Unset): Operational readiness state. Clients should use ready (or rebuilding=false) for
            query readiness; replay watermarks diagnose replay progress but do not replace this signal.
        doc_count (int | Unset): Number of physical vectors or sparse entries visible to the index; chunked indexes may
            contain multiple entries per source document.
        coverage (DerivedCoverageStatus | Unset):
        query_visible_doc_count (int | Unset): Documents currently visible to queries.
        published_doc_count (int | Unset):
        published_node_count (int | Unset):
        root_node (int | Unset):
        published_root_node (int | Unset):
        dense_replay_applied_sequence (int | Unset):
        dense_replay_target_sequence (int | Unset):
        dense_publish_pending (bool | Unset): Whether dense/vector artifacts still need publication before queries see
            the latest data.
        dense_vector_projection_pending (bool | Unset): Whether the shared native exact-vector projection is still being
            built or reconciled. Queries remain correct by falling back to primary embedding artifacts while this is true.
        dense_native_storage_phase (DenseNativeStoragePhase | Unset): Conservative distributed rollout phase for native
            WAL-backed dense-index storage. native_authoritative is reported only when every expected shard has supplied
            current authority evidence.
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
        enrichment_runtime (EnrichmentRuntimeStatus | Unset): Runtime state for the durable embeddings enrichment
            worker.
        hbc_cache (EmbeddingsIndexStatsHbcCache | Unset):
        hbc_posting (EmbeddingsIndexStatsHbcPosting | Unset):
        async_indexing (EmbeddingsIndexStatsAsyncIndexing | Unset):
        projection_checkpoint_status (str | Unset): Durable projection checkpoint status: clean, rebuilding, degraded,
            or repair_required.
        projection_checkpoint_applied_sequence (int | Unset): Highest derived-log sequence covered by the durable
            projection checkpoint.
        projection_checkpoint_generation (int | Unset): Projection generation associated with the durable checkpoint.
        projection_checkpoint_config_fingerprint (str | Unset): Projection configuration identity associated with the
            durable checkpoint.
        checkpoint_replay_tail_sequence_count (int | Unset): Number of derived-log sequences after the durable
            checkpoint that still need replay.
        repair_scan_issue_count (int | Unset): Repair issues found by explicit repair-scan accounting for this
            projection.
        term_count (int | Unset):
        edge_count (int | Unset):
        node_count (int | Unset):
        repair_degraded (bool | Unset):
        repair_issue_count (int | Unset):
        repair_summary_ready (bool | Unset):
        repair_issue_count_estimated (bool | Unset):
        expected_groups (int | Unset):
        reported_groups (int | Unset):
        fresh_groups (int | Unset):
        stale_groups (int | Unset):
        missing_groups (int | Unset):
        unknown_remote_groups (int | Unset):
        resolution (EmbeddingsIndexStatsResolution | Unset): Artifact resolution replay diagnostics.
        promotion (EmbeddingsIndexStatsPromotion | Unset): Artifact promotion replay diagnostics.
    """

    index_type: EmbeddingsIndexStatsIndexType
    readiness: IndexReadinessStatus | Unset = UNSET
    error: str | Unset = UNSET
    total_indexed: int | Unset = UNSET
    disk_usage: int | Unset = UNSET
    total_nodes: int | Unset = UNSET
    total_terms: int | Unset = UNSET
    rebuilding: bool | Unset = UNSET
    repair: IndexRepairStatus | Unset = UNSET
    wal_backlog: int | Unset = UNSET
    backfill_active: bool | Unset = UNSET
    backfill_progress: float | Unset = UNSET
    backfill_items_processed: int | Unset = UNSET
    backfill_state: str | Unset = UNSET
    doc_count: int | Unset = UNSET
    coverage: DerivedCoverageStatus | Unset = UNSET
    query_visible_doc_count: int | Unset = UNSET
    published_doc_count: int | Unset = UNSET
    published_node_count: int | Unset = UNSET
    root_node: int | Unset = UNSET
    published_root_node: int | Unset = UNSET
    dense_replay_applied_sequence: int | Unset = UNSET
    dense_replay_target_sequence: int | Unset = UNSET
    dense_publish_pending: bool | Unset = UNSET
    dense_vector_projection_pending: bool | Unset = UNSET
    dense_native_storage_phase: DenseNativeStoragePhase | Unset = UNSET
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
    enrichment_runtime: EnrichmentRuntimeStatus | Unset = UNSET
    hbc_cache: EmbeddingsIndexStatsHbcCache | Unset = UNSET
    hbc_posting: EmbeddingsIndexStatsHbcPosting | Unset = UNSET
    async_indexing: EmbeddingsIndexStatsAsyncIndexing | Unset = UNSET
    projection_checkpoint_status: str | Unset = UNSET
    projection_checkpoint_applied_sequence: int | Unset = UNSET
    projection_checkpoint_generation: int | Unset = UNSET
    projection_checkpoint_config_fingerprint: str | Unset = UNSET
    checkpoint_replay_tail_sequence_count: int | Unset = UNSET
    repair_scan_issue_count: int | Unset = UNSET
    term_count: int | Unset = UNSET
    edge_count: int | Unset = UNSET
    node_count: int | Unset = UNSET
    repair_degraded: bool | Unset = UNSET
    repair_issue_count: int | Unset = UNSET
    repair_summary_ready: bool | Unset = UNSET
    repair_issue_count_estimated: bool | Unset = UNSET
    expected_groups: int | Unset = UNSET
    reported_groups: int | Unset = UNSET
    fresh_groups: int | Unset = UNSET
    stale_groups: int | Unset = UNSET
    missing_groups: int | Unset = UNSET
    unknown_remote_groups: int | Unset = UNSET
    resolution: EmbeddingsIndexStatsResolution | Unset = UNSET
    promotion: EmbeddingsIndexStatsPromotion | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        readiness: dict[str, Any] | Unset = UNSET
        if not isinstance(self.readiness, Unset):
            readiness = self.readiness.to_dict()

        error = self.error

        total_indexed = self.total_indexed

        disk_usage = self.disk_usage

        total_nodes = self.total_nodes

        total_terms = self.total_terms

        rebuilding = self.rebuilding

        repair: dict[str, Any] | Unset = UNSET
        if not isinstance(self.repair, Unset):
            repair = self.repair.to_dict()

        wal_backlog = self.wal_backlog

        backfill_active = self.backfill_active

        backfill_progress = self.backfill_progress

        backfill_items_processed = self.backfill_items_processed

        backfill_state = self.backfill_state

        doc_count = self.doc_count

        coverage: dict[str, Any] | Unset = UNSET
        if not isinstance(self.coverage, Unset):
            coverage = self.coverage.to_dict()

        query_visible_doc_count = self.query_visible_doc_count

        published_doc_count = self.published_doc_count

        published_node_count = self.published_node_count

        root_node = self.root_node

        published_root_node = self.published_root_node

        dense_replay_applied_sequence = self.dense_replay_applied_sequence

        dense_replay_target_sequence = self.dense_replay_target_sequence

        dense_publish_pending = self.dense_publish_pending

        dense_vector_projection_pending = self.dense_vector_projection_pending

        dense_native_storage_phase: str | Unset = UNSET
        if not isinstance(self.dense_native_storage_phase, Unset):
            dense_native_storage_phase = self.dense_native_storage_phase.value

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

        projection_checkpoint_status = self.projection_checkpoint_status

        projection_checkpoint_applied_sequence = self.projection_checkpoint_applied_sequence

        projection_checkpoint_generation = self.projection_checkpoint_generation

        projection_checkpoint_config_fingerprint = self.projection_checkpoint_config_fingerprint

        checkpoint_replay_tail_sequence_count = self.checkpoint_replay_tail_sequence_count

        repair_scan_issue_count = self.repair_scan_issue_count

        term_count = self.term_count

        edge_count = self.edge_count

        node_count = self.node_count

        repair_degraded = self.repair_degraded

        repair_issue_count = self.repair_issue_count

        repair_summary_ready = self.repair_summary_ready

        repair_issue_count_estimated = self.repair_issue_count_estimated

        expected_groups = self.expected_groups

        reported_groups = self.reported_groups

        fresh_groups = self.fresh_groups

        stale_groups = self.stale_groups

        missing_groups = self.missing_groups

        unknown_remote_groups = self.unknown_remote_groups

        resolution: dict[str, Any] | Unset = UNSET
        if not isinstance(self.resolution, Unset):
            resolution = self.resolution.to_dict()

        promotion: dict[str, Any] | Unset = UNSET
        if not isinstance(self.promotion, Unset):
            promotion = self.promotion.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index_type": index_type,
            }
        )
        if readiness is not UNSET:
            field_dict["readiness"] = readiness
        if error is not UNSET:
            field_dict["error"] = error
        if total_indexed is not UNSET:
            field_dict["total_indexed"] = total_indexed
        if disk_usage is not UNSET:
            field_dict["disk_usage"] = disk_usage
        if total_nodes is not UNSET:
            field_dict["total_nodes"] = total_nodes
        if total_terms is not UNSET:
            field_dict["total_terms"] = total_terms
        if rebuilding is not UNSET:
            field_dict["rebuilding"] = rebuilding
        if repair is not UNSET:
            field_dict["repair"] = repair
        if wal_backlog is not UNSET:
            field_dict["wal_backlog"] = wal_backlog
        if backfill_active is not UNSET:
            field_dict["backfill_active"] = backfill_active
        if backfill_progress is not UNSET:
            field_dict["backfill_progress"] = backfill_progress
        if backfill_items_processed is not UNSET:
            field_dict["backfill_items_processed"] = backfill_items_processed
        if backfill_state is not UNSET:
            field_dict["backfill_state"] = backfill_state
        if doc_count is not UNSET:
            field_dict["doc_count"] = doc_count
        if coverage is not UNSET:
            field_dict["coverage"] = coverage
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
        if dense_vector_projection_pending is not UNSET:
            field_dict["dense_vector_projection_pending"] = dense_vector_projection_pending
        if dense_native_storage_phase is not UNSET:
            field_dict["dense_native_storage_phase"] = dense_native_storage_phase
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
        if projection_checkpoint_status is not UNSET:
            field_dict["projection_checkpoint_status"] = projection_checkpoint_status
        if projection_checkpoint_applied_sequence is not UNSET:
            field_dict["projection_checkpoint_applied_sequence"] = projection_checkpoint_applied_sequence
        if projection_checkpoint_generation is not UNSET:
            field_dict["projection_checkpoint_generation"] = projection_checkpoint_generation
        if projection_checkpoint_config_fingerprint is not UNSET:
            field_dict["projection_checkpoint_config_fingerprint"] = projection_checkpoint_config_fingerprint
        if checkpoint_replay_tail_sequence_count is not UNSET:
            field_dict["checkpoint_replay_tail_sequence_count"] = checkpoint_replay_tail_sequence_count
        if repair_scan_issue_count is not UNSET:
            field_dict["repair_scan_issue_count"] = repair_scan_issue_count
        if term_count is not UNSET:
            field_dict["term_count"] = term_count
        if edge_count is not UNSET:
            field_dict["edge_count"] = edge_count
        if node_count is not UNSET:
            field_dict["node_count"] = node_count
        if repair_degraded is not UNSET:
            field_dict["repair_degraded"] = repair_degraded
        if repair_issue_count is not UNSET:
            field_dict["repair_issue_count"] = repair_issue_count
        if repair_summary_ready is not UNSET:
            field_dict["repair_summary_ready"] = repair_summary_ready
        if repair_issue_count_estimated is not UNSET:
            field_dict["repair_issue_count_estimated"] = repair_issue_count_estimated
        if expected_groups is not UNSET:
            field_dict["expected_groups"] = expected_groups
        if reported_groups is not UNSET:
            field_dict["reported_groups"] = reported_groups
        if fresh_groups is not UNSET:
            field_dict["fresh_groups"] = fresh_groups
        if stale_groups is not UNSET:
            field_dict["stale_groups"] = stale_groups
        if missing_groups is not UNSET:
            field_dict["missing_groups"] = missing_groups
        if unknown_remote_groups is not UNSET:
            field_dict["unknown_remote_groups"] = unknown_remote_groups
        if resolution is not UNSET:
            field_dict["resolution"] = resolution
        if promotion is not UNSET:
            field_dict["promotion"] = promotion

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.derived_coverage_status import DerivedCoverageStatus
        from ..models.embeddings_index_stats_async_indexing import EmbeddingsIndexStatsAsyncIndexing
        from ..models.embeddings_index_stats_hbc_cache import EmbeddingsIndexStatsHbcCache
        from ..models.embeddings_index_stats_hbc_posting import EmbeddingsIndexStatsHbcPosting
        from ..models.embeddings_index_stats_promotion import EmbeddingsIndexStatsPromotion
        from ..models.embeddings_index_stats_resolution import EmbeddingsIndexStatsResolution
        from ..models.enrichment_runtime_status import EnrichmentRuntimeStatus
        from ..models.index_readiness_status import IndexReadinessStatus
        from ..models.index_repair_status import IndexRepairStatus

        d = dict(src_dict)
        index_type = EmbeddingsIndexStatsIndexType(d.pop("index_type"))

        _readiness = d.pop("readiness", UNSET)
        readiness: IndexReadinessStatus | Unset
        if isinstance(_readiness, Unset):
            readiness = UNSET
        else:
            readiness = IndexReadinessStatus.from_dict(_readiness)

        error = d.pop("error", UNSET)

        total_indexed = d.pop("total_indexed", UNSET)

        disk_usage = d.pop("disk_usage", UNSET)

        total_nodes = d.pop("total_nodes", UNSET)

        total_terms = d.pop("total_terms", UNSET)

        rebuilding = d.pop("rebuilding", UNSET)

        _repair = d.pop("repair", UNSET)
        repair: IndexRepairStatus | Unset
        if isinstance(_repair, Unset):
            repair = UNSET
        else:
            repair = IndexRepairStatus.from_dict(_repair)

        wal_backlog = d.pop("wal_backlog", UNSET)

        backfill_active = d.pop("backfill_active", UNSET)

        backfill_progress = d.pop("backfill_progress", UNSET)

        backfill_items_processed = d.pop("backfill_items_processed", UNSET)

        backfill_state = d.pop("backfill_state", UNSET)

        doc_count = d.pop("doc_count", UNSET)

        _coverage = d.pop("coverage", UNSET)
        coverage: DerivedCoverageStatus | Unset
        if isinstance(_coverage, Unset):
            coverage = UNSET
        else:
            coverage = DerivedCoverageStatus.from_dict(_coverage)

        query_visible_doc_count = d.pop("query_visible_doc_count", UNSET)

        published_doc_count = d.pop("published_doc_count", UNSET)

        published_node_count = d.pop("published_node_count", UNSET)

        root_node = d.pop("root_node", UNSET)

        published_root_node = d.pop("published_root_node", UNSET)

        dense_replay_applied_sequence = d.pop("dense_replay_applied_sequence", UNSET)

        dense_replay_target_sequence = d.pop("dense_replay_target_sequence", UNSET)

        dense_publish_pending = d.pop("dense_publish_pending", UNSET)

        dense_vector_projection_pending = d.pop("dense_vector_projection_pending", UNSET)

        _dense_native_storage_phase = d.pop("dense_native_storage_phase", UNSET)
        dense_native_storage_phase: DenseNativeStoragePhase | Unset
        if isinstance(_dense_native_storage_phase, Unset):
            dense_native_storage_phase = UNSET
        else:
            dense_native_storage_phase = DenseNativeStoragePhase(_dense_native_storage_phase)

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
        enrichment_runtime: EnrichmentRuntimeStatus | Unset
        if isinstance(_enrichment_runtime, Unset):
            enrichment_runtime = UNSET
        else:
            enrichment_runtime = EnrichmentRuntimeStatus.from_dict(_enrichment_runtime)

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

        projection_checkpoint_status = d.pop("projection_checkpoint_status", UNSET)

        projection_checkpoint_applied_sequence = d.pop("projection_checkpoint_applied_sequence", UNSET)

        projection_checkpoint_generation = d.pop("projection_checkpoint_generation", UNSET)

        projection_checkpoint_config_fingerprint = d.pop("projection_checkpoint_config_fingerprint", UNSET)

        checkpoint_replay_tail_sequence_count = d.pop("checkpoint_replay_tail_sequence_count", UNSET)

        repair_scan_issue_count = d.pop("repair_scan_issue_count", UNSET)

        term_count = d.pop("term_count", UNSET)

        edge_count = d.pop("edge_count", UNSET)

        node_count = d.pop("node_count", UNSET)

        repair_degraded = d.pop("repair_degraded", UNSET)

        repair_issue_count = d.pop("repair_issue_count", UNSET)

        repair_summary_ready = d.pop("repair_summary_ready", UNSET)

        repair_issue_count_estimated = d.pop("repair_issue_count_estimated", UNSET)

        expected_groups = d.pop("expected_groups", UNSET)

        reported_groups = d.pop("reported_groups", UNSET)

        fresh_groups = d.pop("fresh_groups", UNSET)

        stale_groups = d.pop("stale_groups", UNSET)

        missing_groups = d.pop("missing_groups", UNSET)

        unknown_remote_groups = d.pop("unknown_remote_groups", UNSET)

        _resolution = d.pop("resolution", UNSET)
        resolution: EmbeddingsIndexStatsResolution | Unset
        if isinstance(_resolution, Unset):
            resolution = UNSET
        else:
            resolution = EmbeddingsIndexStatsResolution.from_dict(_resolution)

        _promotion = d.pop("promotion", UNSET)
        promotion: EmbeddingsIndexStatsPromotion | Unset
        if isinstance(_promotion, Unset):
            promotion = UNSET
        else:
            promotion = EmbeddingsIndexStatsPromotion.from_dict(_promotion)

        embeddings_index_stats = cls(
            index_type=index_type,
            readiness=readiness,
            error=error,
            total_indexed=total_indexed,
            disk_usage=disk_usage,
            total_nodes=total_nodes,
            total_terms=total_terms,
            rebuilding=rebuilding,
            repair=repair,
            wal_backlog=wal_backlog,
            backfill_active=backfill_active,
            backfill_progress=backfill_progress,
            backfill_items_processed=backfill_items_processed,
            backfill_state=backfill_state,
            doc_count=doc_count,
            coverage=coverage,
            query_visible_doc_count=query_visible_doc_count,
            published_doc_count=published_doc_count,
            published_node_count=published_node_count,
            root_node=root_node,
            published_root_node=published_root_node,
            dense_replay_applied_sequence=dense_replay_applied_sequence,
            dense_replay_target_sequence=dense_replay_target_sequence,
            dense_publish_pending=dense_publish_pending,
            dense_vector_projection_pending=dense_vector_projection_pending,
            dense_native_storage_phase=dense_native_storage_phase,
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
            projection_checkpoint_status=projection_checkpoint_status,
            projection_checkpoint_applied_sequence=projection_checkpoint_applied_sequence,
            projection_checkpoint_generation=projection_checkpoint_generation,
            projection_checkpoint_config_fingerprint=projection_checkpoint_config_fingerprint,
            checkpoint_replay_tail_sequence_count=checkpoint_replay_tail_sequence_count,
            repair_scan_issue_count=repair_scan_issue_count,
            term_count=term_count,
            edge_count=edge_count,
            node_count=node_count,
            repair_degraded=repair_degraded,
            repair_issue_count=repair_issue_count,
            repair_summary_ready=repair_summary_ready,
            repair_issue_count_estimated=repair_issue_count_estimated,
            expected_groups=expected_groups,
            reported_groups=reported_groups,
            fresh_groups=fresh_groups,
            stale_groups=stale_groups,
            missing_groups=missing_groups,
            unknown_remote_groups=unknown_remote_groups,
            resolution=resolution,
            promotion=promotion,
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
