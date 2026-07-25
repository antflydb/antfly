from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="EnrichmentRuntimeStatus")


@_attrs_define
class EnrichmentRuntimeStatus:
    """Runtime state for the durable embeddings enrichment worker.

    Attributes:
        enabled (bool):
        target_sequence (int):
        applied_sequence (int):
        pending_sequence_count (int):
        projection_checkpoint_status (str):
        projection_checkpoint_applied_sequence (int):
        projection_checkpoint_generation (int):
        projection_checkpoint_config_fingerprint (str):
        projection_checkpoint_identity_consistent (bool): Whether every shard contributing to this status reports the
            same checkpoint generation and configuration identity.
        checkpoint_replay_tail_sequence_count (int):
        processed_requests (int):
        error_count (int):
        retryable_error_count (int):
        fatal_error_count (int):
        retrying (bool):
        worker_failed (bool):
        worker_started (bool): Whether the background enrichment worker is currently running.
        stalled (bool): Whether work is pending with no running worker, retry, or terminal failure explaining the
            backlog.
        skip_by_hash_count (int):
        skipped_source_count (int):
        codec_decode_failures (int):
        embed_batches_started (int):
        embed_batches_completed (int):
        embed_items_started (int):
        embed_items_completed (int):
        active_embed_batch_items (int):
        active_embed_batch_bytes (int):
        active_embed_batch_max_bytes (int):
        active_embed_batch_started_ms (int):
        last_embed_batch_items (int):
        last_embed_batch_bytes (int):
        last_embed_batch_max_bytes (int):
        last_embed_batch_ns (int):
        total_embed_ns (int):
    """

    enabled: bool
    target_sequence: int
    applied_sequence: int
    pending_sequence_count: int
    projection_checkpoint_status: str
    projection_checkpoint_applied_sequence: int
    projection_checkpoint_generation: int
    projection_checkpoint_config_fingerprint: str
    projection_checkpoint_identity_consistent: bool
    checkpoint_replay_tail_sequence_count: int
    processed_requests: int
    error_count: int
    retryable_error_count: int
    fatal_error_count: int
    retrying: bool
    worker_failed: bool
    worker_started: bool
    stalled: bool
    skip_by_hash_count: int
    skipped_source_count: int
    codec_decode_failures: int
    embed_batches_started: int
    embed_batches_completed: int
    embed_items_started: int
    embed_items_completed: int
    active_embed_batch_items: int
    active_embed_batch_bytes: int
    active_embed_batch_max_bytes: int
    active_embed_batch_started_ms: int
    last_embed_batch_items: int
    last_embed_batch_bytes: int
    last_embed_batch_max_bytes: int
    last_embed_batch_ns: int
    total_embed_ns: int

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        target_sequence = self.target_sequence

        applied_sequence = self.applied_sequence

        pending_sequence_count = self.pending_sequence_count

        projection_checkpoint_status = self.projection_checkpoint_status

        projection_checkpoint_applied_sequence = self.projection_checkpoint_applied_sequence

        projection_checkpoint_generation = self.projection_checkpoint_generation

        projection_checkpoint_config_fingerprint = self.projection_checkpoint_config_fingerprint

        projection_checkpoint_identity_consistent = self.projection_checkpoint_identity_consistent

        checkpoint_replay_tail_sequence_count = self.checkpoint_replay_tail_sequence_count

        processed_requests = self.processed_requests

        error_count = self.error_count

        retryable_error_count = self.retryable_error_count

        fatal_error_count = self.fatal_error_count

        retrying = self.retrying

        worker_failed = self.worker_failed

        worker_started = self.worker_started

        stalled = self.stalled

        skip_by_hash_count = self.skip_by_hash_count

        skipped_source_count = self.skipped_source_count

        codec_decode_failures = self.codec_decode_failures

        embed_batches_started = self.embed_batches_started

        embed_batches_completed = self.embed_batches_completed

        embed_items_started = self.embed_items_started

        embed_items_completed = self.embed_items_completed

        active_embed_batch_items = self.active_embed_batch_items

        active_embed_batch_bytes = self.active_embed_batch_bytes

        active_embed_batch_max_bytes = self.active_embed_batch_max_bytes

        active_embed_batch_started_ms = self.active_embed_batch_started_ms

        last_embed_batch_items = self.last_embed_batch_items

        last_embed_batch_bytes = self.last_embed_batch_bytes

        last_embed_batch_max_bytes = self.last_embed_batch_max_bytes

        last_embed_batch_ns = self.last_embed_batch_ns

        total_embed_ns = self.total_embed_ns

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "enabled": enabled,
                "target_sequence": target_sequence,
                "applied_sequence": applied_sequence,
                "pending_sequence_count": pending_sequence_count,
                "projection_checkpoint_status": projection_checkpoint_status,
                "projection_checkpoint_applied_sequence": projection_checkpoint_applied_sequence,
                "projection_checkpoint_generation": projection_checkpoint_generation,
                "projection_checkpoint_config_fingerprint": projection_checkpoint_config_fingerprint,
                "projection_checkpoint_identity_consistent": projection_checkpoint_identity_consistent,
                "checkpoint_replay_tail_sequence_count": checkpoint_replay_tail_sequence_count,
                "processed_requests": processed_requests,
                "error_count": error_count,
                "retryable_error_count": retryable_error_count,
                "fatal_error_count": fatal_error_count,
                "retrying": retrying,
                "worker_failed": worker_failed,
                "worker_started": worker_started,
                "stalled": stalled,
                "skip_by_hash_count": skip_by_hash_count,
                "skipped_source_count": skipped_source_count,
                "codec_decode_failures": codec_decode_failures,
                "embed_batches_started": embed_batches_started,
                "embed_batches_completed": embed_batches_completed,
                "embed_items_started": embed_items_started,
                "embed_items_completed": embed_items_completed,
                "active_embed_batch_items": active_embed_batch_items,
                "active_embed_batch_bytes": active_embed_batch_bytes,
                "active_embed_batch_max_bytes": active_embed_batch_max_bytes,
                "active_embed_batch_started_ms": active_embed_batch_started_ms,
                "last_embed_batch_items": last_embed_batch_items,
                "last_embed_batch_bytes": last_embed_batch_bytes,
                "last_embed_batch_max_bytes": last_embed_batch_max_bytes,
                "last_embed_batch_ns": last_embed_batch_ns,
                "total_embed_ns": total_embed_ns,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enabled = d.pop("enabled")

        target_sequence = d.pop("target_sequence")

        applied_sequence = d.pop("applied_sequence")

        pending_sequence_count = d.pop("pending_sequence_count")

        projection_checkpoint_status = d.pop("projection_checkpoint_status")

        projection_checkpoint_applied_sequence = d.pop("projection_checkpoint_applied_sequence")

        projection_checkpoint_generation = d.pop("projection_checkpoint_generation")

        projection_checkpoint_config_fingerprint = d.pop("projection_checkpoint_config_fingerprint")

        projection_checkpoint_identity_consistent = d.pop("projection_checkpoint_identity_consistent")

        checkpoint_replay_tail_sequence_count = d.pop("checkpoint_replay_tail_sequence_count")

        processed_requests = d.pop("processed_requests")

        error_count = d.pop("error_count")

        retryable_error_count = d.pop("retryable_error_count")

        fatal_error_count = d.pop("fatal_error_count")

        retrying = d.pop("retrying")

        worker_failed = d.pop("worker_failed")

        worker_started = d.pop("worker_started")

        stalled = d.pop("stalled")

        skip_by_hash_count = d.pop("skip_by_hash_count")

        skipped_source_count = d.pop("skipped_source_count")

        codec_decode_failures = d.pop("codec_decode_failures")

        embed_batches_started = d.pop("embed_batches_started")

        embed_batches_completed = d.pop("embed_batches_completed")

        embed_items_started = d.pop("embed_items_started")

        embed_items_completed = d.pop("embed_items_completed")

        active_embed_batch_items = d.pop("active_embed_batch_items")

        active_embed_batch_bytes = d.pop("active_embed_batch_bytes")

        active_embed_batch_max_bytes = d.pop("active_embed_batch_max_bytes")

        active_embed_batch_started_ms = d.pop("active_embed_batch_started_ms")

        last_embed_batch_items = d.pop("last_embed_batch_items")

        last_embed_batch_bytes = d.pop("last_embed_batch_bytes")

        last_embed_batch_max_bytes = d.pop("last_embed_batch_max_bytes")

        last_embed_batch_ns = d.pop("last_embed_batch_ns")

        total_embed_ns = d.pop("total_embed_ns")

        enrichment_runtime_status = cls(
            enabled=enabled,
            target_sequence=target_sequence,
            applied_sequence=applied_sequence,
            pending_sequence_count=pending_sequence_count,
            projection_checkpoint_status=projection_checkpoint_status,
            projection_checkpoint_applied_sequence=projection_checkpoint_applied_sequence,
            projection_checkpoint_generation=projection_checkpoint_generation,
            projection_checkpoint_config_fingerprint=projection_checkpoint_config_fingerprint,
            projection_checkpoint_identity_consistent=projection_checkpoint_identity_consistent,
            checkpoint_replay_tail_sequence_count=checkpoint_replay_tail_sequence_count,
            processed_requests=processed_requests,
            error_count=error_count,
            retryable_error_count=retryable_error_count,
            fatal_error_count=fatal_error_count,
            retrying=retrying,
            worker_failed=worker_failed,
            worker_started=worker_started,
            stalled=stalled,
            skip_by_hash_count=skip_by_hash_count,
            skipped_source_count=skipped_source_count,
            codec_decode_failures=codec_decode_failures,
            embed_batches_started=embed_batches_started,
            embed_batches_completed=embed_batches_completed,
            embed_items_started=embed_items_started,
            embed_items_completed=embed_items_completed,
            active_embed_batch_items=active_embed_batch_items,
            active_embed_batch_bytes=active_embed_batch_bytes,
            active_embed_batch_max_bytes=active_embed_batch_max_bytes,
            active_embed_batch_started_ms=active_embed_batch_started_ms,
            last_embed_batch_items=last_embed_batch_items,
            last_embed_batch_bytes=last_embed_batch_bytes,
            last_embed_batch_max_bytes=last_embed_batch_max_bytes,
            last_embed_batch_ns=last_embed_batch_ns,
            total_embed_ns=total_embed_ns,
        )

        return enrichment_runtime_status
