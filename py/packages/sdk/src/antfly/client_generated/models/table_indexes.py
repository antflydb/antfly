from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.created_algebraic_index import CreatedAlgebraicIndex
    from ..models.created_embeddings_index import CreatedEmbeddingsIndex
    from ..models.created_full_text_index import CreatedFullTextIndex
    from ..models.created_graph_index import CreatedGraphIndex


T = TypeVar("T", bound="TableIndexes")


@_attrs_define
class TableIndexes:
    """ """

    additional_properties: dict[
        str, CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex
    ] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.created_embeddings_index import CreatedEmbeddingsIndex
        from ..models.created_full_text_index import CreatedFullTextIndex
        from ..models.created_graph_index import CreatedGraphIndex

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, CreatedFullTextIndex):
                field_dict[prop_name] = prop.to_dict()
            elif isinstance(prop, CreatedEmbeddingsIndex):
                field_dict[prop_name] = prop.to_dict()
            elif isinstance(prop, CreatedGraphIndex):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.created_algebraic_index import CreatedAlgebraicIndex
        from ..models.created_embeddings_index import CreatedEmbeddingsIndex
        from ..models.created_full_text_index import CreatedFullTextIndex
        from ..models.created_graph_index import CreatedGraphIndex

        d = dict(src_dict)
        table_indexes = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(
                data: object,
            ) -> CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_created_index_type_0 = CreatedFullTextIndex.from_dict(data)

                    return componentsschemas_created_index_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_created_index_type_1 = CreatedEmbeddingsIndex.from_dict(data)

                    return componentsschemas_created_index_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_created_index_type_2 = CreatedGraphIndex.from_dict(data)

                    return componentsschemas_created_index_type_2
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_created_index_type_3 = CreatedAlgebraicIndex.from_dict(data)

                return componentsschemas_created_index_type_3

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        table_indexes.additional_properties = additional_properties
        return table_indexes

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(
        self, key: str
    ) -> CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex:
        return self.additional_properties[key]

    def __setitem__(
        self, key: str, value: CreatedAlgebraicIndex | CreatedEmbeddingsIndex | CreatedFullTextIndex | CreatedGraphIndex
    ) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
