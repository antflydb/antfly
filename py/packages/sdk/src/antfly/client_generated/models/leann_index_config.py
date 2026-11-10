from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

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
            covers them. "transient" (the default when possible) keeps only a small stub per document and re-embeds a few
            candidates per query from the document's field or template through the index's embedder, so the index stores
            about 15% of the raw vectors' bytes. "persistent" keeps every embedding, and is the only choice for external
            vectors, chunked indexes, indexes fed by other enrichments, and the pq quantizer.
        search_ef (int | Unset): Beam width for searches. 0 picks the default for the quantizer (512 for rabitq, 64 for
            pq). Default: 0.
        bound_scale (float | Unset): Multiplier on RaBitQ error bounds (rabitq only). Larger recomputes more candidates
            and loses fewer true neighbors. Default: 1.0.
        batch_size (int | Unset): Candidates recomputed per batch. 0 uses k. Default: 0.
    """

    max_degree: int | Unset = 30
    quantizer: LeannIndexConfigQuantizer | Unset = LeannIndexConfigQuantizer.RABITQ
    vector_storage: LeannIndexConfigVectorStorage | Unset = UNSET
    search_ef: int | Unset = 0
    bound_scale: float | Unset = 1.0
    batch_size: int | Unset = 0

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

        leann_index_config = cls(
            max_degree=max_degree,
            quantizer=quantizer,
            vector_storage=vector_storage,
            search_ef=search_ef,
            bound_scale=bound_scale,
            batch_size=batch_size,
        )

        return leann_index_config
