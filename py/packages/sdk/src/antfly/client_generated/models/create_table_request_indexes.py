from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.create_algebraic_index_request import CreateAlgebraicIndexRequest
    from ..models.create_embeddings_index_request import CreateEmbeddingsIndexRequest
    from ..models.create_full_text_index_request import CreateFullTextIndexRequest
    from ..models.create_graph_index_request import CreateGraphIndexRequest


T = TypeVar("T", bound="CreateTableRequestIndexes")


@_attrs_define
class CreateTableRequestIndexes:
    """Map of index name to create-index configuration. The map key owns the
    index name; do not repeat `name` inside the configuration. Indexes enable
    different query capabilities:
    - Full-text indexes for BM25 search
    - Vector indexes for semantic similarity
    - Multimodal indexes for images/audio/video

    You can add multiple indexes to support different query patterns.

        Example:
            {'search_index': {'type': 'full_text'}, 'embedding_index': {'type': 'embeddings', 'dimension': 384, 'embedder':
                {'provider': 'ollama', 'model': 'all-minilm'}}}

    """

    additional_properties: dict[
        str,
        CreateAlgebraicIndexRequest
        | CreateEmbeddingsIndexRequest
        | CreateFullTextIndexRequest
        | CreateGraphIndexRequest,
    ] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.create_embeddings_index_request import CreateEmbeddingsIndexRequest
        from ..models.create_full_text_index_request import CreateFullTextIndexRequest
        from ..models.create_graph_index_request import CreateGraphIndexRequest

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, CreateFullTextIndexRequest):
                field_dict[prop_name] = prop.to_dict()
            elif isinstance(prop, CreateEmbeddingsIndexRequest):
                field_dict[prop_name] = prop.to_dict()
            elif isinstance(prop, CreateGraphIndexRequest):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.create_algebraic_index_request import CreateAlgebraicIndexRequest
        from ..models.create_embeddings_index_request import CreateEmbeddingsIndexRequest
        from ..models.create_full_text_index_request import CreateFullTextIndexRequest
        from ..models.create_graph_index_request import CreateGraphIndexRequest

        d = dict(src_dict)
        create_table_request_indexes = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(
                data: object,
            ) -> (
                CreateAlgebraicIndexRequest
                | CreateEmbeddingsIndexRequest
                | CreateFullTextIndexRequest
                | CreateGraphIndexRequest
            ):
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_create_index_request_type_0 = CreateFullTextIndexRequest.from_dict(data)

                    return componentsschemas_create_index_request_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_create_index_request_type_1 = CreateEmbeddingsIndexRequest.from_dict(data)

                    return componentsschemas_create_index_request_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_create_index_request_type_2 = CreateGraphIndexRequest.from_dict(data)

                    return componentsschemas_create_index_request_type_2
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_create_index_request_type_3 = CreateAlgebraicIndexRequest.from_dict(data)

                return componentsschemas_create_index_request_type_3

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        create_table_request_indexes.additional_properties = additional_properties
        return create_table_request_indexes

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(
        self, key: str
    ) -> (
        CreateAlgebraicIndexRequest
        | CreateEmbeddingsIndexRequest
        | CreateFullTextIndexRequest
        | CreateGraphIndexRequest
    ):
        return self.additional_properties[key]

    def __setitem__(
        self,
        key: str,
        value: CreateAlgebraicIndexRequest
        | CreateEmbeddingsIndexRequest
        | CreateFullTextIndexRequest
        | CreateGraphIndexRequest,
    ) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
