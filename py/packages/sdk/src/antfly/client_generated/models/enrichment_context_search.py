from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_context_search_vector import EnrichmentContextSearchVector


T = TypeVar("T", bound="EnrichmentContextSearch")


@_attrs_define
class EnrichmentContextSearch:
    """Ranked retrieval (requires `tracking: snapshot`): either full-text relevance (`text` + `text_field`) or dense k-NN
    (`vector` binding + `vector_index`). Members render in rank order.

        Attributes:
            table (str | Unset):
            text (Any | Unset): Query text literal or `{"$source":"/pointer"}`.
            text_field (str | Unset):
            vector (EnrichmentContextSearchVector | Unset): `{"$source":"/pointer"}` to the query vector in the dependent
                row.
            vector_index (str | Unset):
            k (int | Unset):  Default: 10.
    """

    table: str | Unset = UNSET
    text: Any | Unset = UNSET
    text_field: str | Unset = UNSET
    vector: EnrichmentContextSearchVector | Unset = UNSET
    vector_index: str | Unset = UNSET
    k: int | Unset = 10

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        text = self.text

        text_field = self.text_field

        vector: dict[str, Any] | Unset = UNSET
        if not isinstance(self.vector, Unset):
            vector = self.vector.to_dict()

        vector_index = self.vector_index

        k = self.k

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if table is not UNSET:
            field_dict["table"] = table
        if text is not UNSET:
            field_dict["text"] = text
        if text_field is not UNSET:
            field_dict["text_field"] = text_field
        if vector is not UNSET:
            field_dict["vector"] = vector
        if vector_index is not UNSET:
            field_dict["vector_index"] = vector_index
        if k is not UNSET:
            field_dict["k"] = k

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_context_search_vector import EnrichmentContextSearchVector

        d = dict(src_dict)
        table = d.pop("table", UNSET)

        text = d.pop("text", UNSET)

        text_field = d.pop("text_field", UNSET)

        _vector = d.pop("vector", UNSET)
        vector: EnrichmentContextSearchVector | Unset
        if isinstance(_vector, Unset):
            vector = UNSET
        else:
            vector = EnrichmentContextSearchVector.from_dict(_vector)

        vector_index = d.pop("vector_index", UNSET)

        k = d.pop("k", UNSET)

        enrichment_context_search = cls(
            table=table,
            text=text,
            text_field=text_field,
            vector=vector,
            vector_index=vector_index,
            k=k,
        )

        return enrichment_context_search
