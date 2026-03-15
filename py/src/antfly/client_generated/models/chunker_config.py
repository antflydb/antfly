from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunker_provider import ChunkerProvider
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chunker_config_full_text_index import ChunkerConfigFullTextIndex


T = TypeVar("T", bound="ChunkerConfig")


@_attrs_define
class ChunkerConfig:
    """A unified configuration for a chunking provider.

    Example:
        {'provider': 'termite', 'model': 'fixed', 'text': {'target_tokens': 500, 'overlap_tokens': 50}}

    Attributes:
        provider (ChunkerProvider): The chunking provider to use.
        store_chunks (Union[Unset, bool]): Controls whether chunk data is persisted to storage. When false (default),
            chunks are generated in memory and only embeddings are stored. When true, both chunks and embeddings are stored.
            Default: False.
        full_text_index (Union[Unset, ChunkerConfigFullTextIndex]): Configuration for full-text indexing of chunks in
            Bleve.
            When present (even if empty), chunks will be stored with :cft: suffix and indexed in Bleve's _chunks field.
            When absent, chunks use :c: suffix and are only used for vector embeddings.
    """

    provider: ChunkerProvider
    store_chunks: Union[Unset, bool] = False
    full_text_index: Union[Unset, "ChunkerConfigFullTextIndex"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        store_chunks = self.store_chunks

        full_text_index: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.full_text_index, Unset):
            full_text_index = self.full_text_index.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if store_chunks is not UNSET:
            field_dict["store_chunks"] = store_chunks
        if full_text_index is not UNSET:
            field_dict["full_text_index"] = full_text_index

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunker_config_full_text_index import ChunkerConfigFullTextIndex

        d = dict(src_dict)
        provider = ChunkerProvider(d.pop("provider"))

        store_chunks = d.pop("store_chunks", UNSET)

        _full_text_index = d.pop("full_text_index", UNSET)
        full_text_index: Union[Unset, ChunkerConfigFullTextIndex]
        if isinstance(_full_text_index, Unset):
            full_text_index = UNSET
        else:
            full_text_index = ChunkerConfigFullTextIndex.from_dict(_full_text_index)

        chunker_config = cls(
            provider=provider,
            store_chunks=store_chunks,
            full_text_index=full_text_index,
        )

        chunker_config.additional_properties = d
        return chunker_config

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
