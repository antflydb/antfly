from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from dateutil.parser import isoparse

from ..models.embedding_index_activity_phase import EmbeddingIndexActivityPhase

T = TypeVar("T", bound="EmbeddingIndexActivity")


@_attrs_define
class EmbeddingIndexActivity:
    """Volatile index-incarnation activity. It explains motion but never participates in readiness.

    Attributes:
        epoch (str): Opaque worker-and-index-incarnation identity. Rates are valid only between samples with the same
            epoch.
        phase (EmbeddingIndexActivityPhase):
        chunks_created (int): Chunks created for this index during the activity epoch.
        embedding_batches_completed (int):
        embeddings_computed (int): Embedding vectors successfully computed for this index during the activity epoch.
        active_batch_size (int): Items currently submitted to an embedding provider for this index.
        last_progress_at (datetime.datetime | None): Completion time of the latest successful embedding batch, or null
            before the first batch.
    """

    epoch: str
    phase: EmbeddingIndexActivityPhase
    chunks_created: int
    embedding_batches_completed: int
    embeddings_computed: int
    active_batch_size: int
    last_progress_at: datetime.datetime | None

    def to_dict(self) -> dict[str, Any]:
        epoch = self.epoch

        phase = self.phase.value

        chunks_created = self.chunks_created

        embedding_batches_completed = self.embedding_batches_completed

        embeddings_computed = self.embeddings_computed

        active_batch_size = self.active_batch_size

        last_progress_at: None | str
        if isinstance(self.last_progress_at, datetime.datetime):
            last_progress_at = self.last_progress_at.isoformat()
        else:
            last_progress_at = self.last_progress_at

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "epoch": epoch,
                "phase": phase,
                "chunks_created": chunks_created,
                "embedding_batches_completed": embedding_batches_completed,
                "embeddings_computed": embeddings_computed,
                "active_batch_size": active_batch_size,
                "last_progress_at": last_progress_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        epoch = d.pop("epoch")

        phase = EmbeddingIndexActivityPhase(d.pop("phase"))

        chunks_created = d.pop("chunks_created")

        embedding_batches_completed = d.pop("embedding_batches_completed")

        embeddings_computed = d.pop("embeddings_computed")

        active_batch_size = d.pop("active_batch_size")

        def _parse_last_progress_at(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                last_progress_at_type_0 = isoparse(data)

                return last_progress_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        last_progress_at = _parse_last_progress_at(d.pop("last_progress_at"))

        embedding_index_activity = cls(
            epoch=epoch,
            phase=phase,
            chunks_created=chunks_created,
            embedding_batches_completed=embedding_batches_completed,
            embeddings_computed=embeddings_computed,
            active_batch_size=active_batch_size,
            last_progress_at=last_progress_at,
        )

        return embedding_index_activity
