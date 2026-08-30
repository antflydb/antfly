from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="DenseVectorPublicationStatus")


@_attrs_define
class DenseVectorPublicationStatus:
    """Exact dense-vector publication cardinality for the observed index incarnation.

    Attributes:
        target_vectors (int): Exact durable vector target for the current dense-index incarnation.
        searchable_vectors (int): Physical vectors currently visible to queries.
        complete (bool): Whether searchable_vectors exactly equals target_vectors.
    """

    target_vectors: int
    searchable_vectors: int
    complete: bool

    def to_dict(self) -> dict[str, Any]:
        target_vectors = self.target_vectors

        searchable_vectors = self.searchable_vectors

        complete = self.complete

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "target_vectors": target_vectors,
                "searchable_vectors": searchable_vectors,
                "complete": complete,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        target_vectors = d.pop("target_vectors")

        searchable_vectors = d.pop("searchable_vectors")

        complete = d.pop("complete")

        dense_vector_publication_status = cls(
            target_vectors=target_vectors,
            searchable_vectors=searchable_vectors,
            complete=complete,
        )

        return dense_vector_publication_status
