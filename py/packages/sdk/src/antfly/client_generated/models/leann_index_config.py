from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.leann_index_config_media_vectors import LeannIndexConfigMediaVectors
from ..models.leann_index_config_quantizer import LeannIndexConfigQuantizer
from ..models.leann_index_config_vector_storage import LeannIndexConfigVectorStorage
from ..types import UNSET, Unset

T = TypeVar("T", bound="LeannIndexConfig")


@_attrs_define
class LeannIndexConfig:
    """Tuning for algorithm=leann dense indexes. All fields are optional.

    Attributes:
        max_degree (int | Unset): Graph degree cap M. Most nodes keep M/5 neighbors after pruning; hubs keep up to M.
            Default: 30.
        quantizer (LeannIndexConfigQuantizer | Unset): Compact code used to steer the graph walk. "rabitq" (default)
            carries error bounds, so only candidates that can still reach the top k are recomputed. "pq" follows the LEANN
            paper's fixed rerank ratio; prefer it only for very low dimensions (below about 64). Default:
            LeannIndexConfigQuantizer.RABITQ.
        vector_storage (LeannIndexConfigVectorStorage | Unset): Whether document embeddings stay stored after the graph
            covers them. "auto" (the default when the index has its own embedder) keeps only a small stub for each embedding
            computed from text (a field, a text-only template, or a text chunk) and re-embeds a few such candidates per
            query, while embeddings computed from media (images, audio windows, model outputs) stay stored. "transient"
            stubs every covered embedding, so it requires inputs that re-embed from text: no media chunkers. "persistent"
            keeps every embedding, and is the only choice for external vectors, indexes fed by other enrichments, and the pq
            quantizer.
        search_ef (int | Unset): Beam width for searches. 0 picks the default for the quantizer (512 for rabitq, 64 for
            pq). Default: 0.
        bound_scale (float | Unset): Multiplier on RaBitQ error bounds (rabitq only). Larger recomputes more candidates
            and loses fewer true neighbors. Default: 1.0.
        batch_size (int | Unset): Candidates recomputed per batch. 0 uses k. Default: 0.
        reembed_budget_ms (int | Unset): Time a query may spend re-embedding candidates whose stored vectors were
            dropped. If the budget is spent before scoring finishes, the search returns an incomplete-coverage error;
            retry with a larger budget. 0 means no limit. Default: 0.
        media_vectors (LeannIndexConfigMediaVectors | Unset): How vectors that cannot be re-embedded from text (images,
            audio windows, model outputs) are kept once the graph covers them. "full" keeps the stored float32 vector.
            "int8" replaces it with a stub holding an int8 copy (about a quarter of the size), read back instead of re-
            embedding. Requires "auto" or "transient" storage. Default: LeannIndexConfigMediaVectors.FULL.
    """

    max_degree: int | Unset = 30
    quantizer: LeannIndexConfigQuantizer | Unset = LeannIndexConfigQuantizer.RABITQ
    vector_storage: LeannIndexConfigVectorStorage | Unset = UNSET
    search_ef: int | Unset = 0
    bound_scale: float | Unset = 1.0
    batch_size: int | Unset = 0
    reembed_budget_ms: int | Unset = 0
    media_vectors: LeannIndexConfigMediaVectors | Unset = LeannIndexConfigMediaVectors.FULL

    def to_dict(self) -> dict[str, Any]:
        max_degree = self.max_degree

        quantizer: str | Unset = UNSET
        if not isinstance(self.quantizer, Unset):
            quantizer = self.quantizer.value

        vector_storage: str | Unset = UNSET
        if not isinstance(self.vector_storage, Unset):
            vector_storage = self.vector_storage.value

        search_ef = self.search_ef

        bound_scale = self.bound_scale

        batch_size = self.batch_size

        reembed_budget_ms = self.reembed_budget_ms

        media_vectors: str | Unset = UNSET
        if not isinstance(self.media_vectors, Unset):
            media_vectors = self.media_vectors.value

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_degree is not UNSET:
            field_dict["max_degree"] = max_degree
        if quantizer is not UNSET:
            field_dict["quantizer"] = quantizer
        if vector_storage is not UNSET:
            field_dict["vector_storage"] = vector_storage
        if search_ef is not UNSET:
            field_dict["search_ef"] = search_ef
        if bound_scale is not UNSET:
            field_dict["bound_scale"] = bound_scale
        if batch_size is not UNSET:
            field_dict["batch_size"] = batch_size
        if reembed_budget_ms is not UNSET:
            field_dict["reembed_budget_ms"] = reembed_budget_ms
        if media_vectors is not UNSET:
            field_dict["media_vectors"] = media_vectors

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_degree = d.pop("max_degree", UNSET)

        _quantizer = d.pop("quantizer", UNSET)
        quantizer: LeannIndexConfigQuantizer | Unset
        if isinstance(_quantizer, Unset):
            quantizer = UNSET
        else:
            quantizer = LeannIndexConfigQuantizer(_quantizer)

        _vector_storage = d.pop("vector_storage", UNSET)
        vector_storage: LeannIndexConfigVectorStorage | Unset
        if isinstance(_vector_storage, Unset):
            vector_storage = UNSET
        else:
            vector_storage = LeannIndexConfigVectorStorage(_vector_storage)

        search_ef = d.pop("search_ef", UNSET)

        bound_scale = d.pop("bound_scale", UNSET)

        batch_size = d.pop("batch_size", UNSET)

        reembed_budget_ms = d.pop("reembed_budget_ms", UNSET)

        _media_vectors = d.pop("media_vectors", UNSET)
        media_vectors: LeannIndexConfigMediaVectors | Unset
        if isinstance(_media_vectors, Unset):
            media_vectors = UNSET
        else:
            media_vectors = LeannIndexConfigMediaVectors(_media_vectors)

        leann_index_config = cls(
            max_degree=max_degree,
            quantizer=quantizer,
            vector_storage=vector_storage,
            search_ef=search_ef,
            bound_scale=bound_scale,
            batch_size=batch_size,
            reembed_budget_ms=reembed_budget_ms,
            media_vectors=media_vectors,
        )

        return leann_index_config
