from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.embedding_type_1 import EmbeddingType1
    from ..models.embedding_type_3 import EmbeddingType3


T = TypeVar("T", bound="QueryRequestEmbeddings")


@_attrs_define
class QueryRequestEmbeddings:
    """Pre-computed embeddings to use for semantic searches instead of embedding the semantic_search string.
    The keys are the index names. Values can be either:
    - **Dense (array)**: an array of floats, e.g. `[0.1, 0.2, 0.3]`
    - **Dense (packed)**: a base64 string of little-endian float32 bytes (~4x more compact)
    - **Sparse**: an object with `indices` (array of ints) and `values` (array of floats),
      e.g. `{"indices": [1, 5, 100], "values": [0.3, 0.7, 0.1]}`
    - **Sparse (packed)**: an object with `packed_indices` (base64 uint32 LE) and `packed_values` (base64 float32 LE)

    Use when you've already generated embeddings on the client side to avoid redundant embedding calls.

    """

    additional_properties: dict[str, EmbeddingType1 | EmbeddingType3 | list[float] | str] = _attrs_field(
        init=False, factory=dict
    )

    def to_dict(self) -> dict[str, Any]:
        from ..models.embedding_type_1 import EmbeddingType1
        from ..models.embedding_type_3 import EmbeddingType3

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, list):
                field_dict[prop_name] = prop

            elif isinstance(prop, EmbeddingType1):
                field_dict[prop_name] = prop.to_dict()
            elif isinstance(prop, EmbeddingType3):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.embedding_type_1 import EmbeddingType1
        from ..models.embedding_type_3 import EmbeddingType3

        d = dict(src_dict)
        query_request_embeddings = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(data: object) -> EmbeddingType1 | EmbeddingType3 | list[float] | str:
                try:
                    if not isinstance(data, list):
                        raise TypeError()
                    componentsschemas_embedding_type_0 = cast(list[float], data)

                    return componentsschemas_embedding_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_embedding_type_1 = EmbeddingType1.from_dict(data)

                    return componentsschemas_embedding_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_embedding_type_3 = EmbeddingType3.from_dict(data)

                    return componentsschemas_embedding_type_3
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                return cast(EmbeddingType1 | EmbeddingType3 | list[float] | str, data)

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        query_request_embeddings.additional_properties = additional_properties
        return query_request_embeddings

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> EmbeddingType1 | EmbeddingType3 | list[float] | str:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: EmbeddingType1 | EmbeddingType3 | list[float] | str) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
