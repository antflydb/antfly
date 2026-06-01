from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.algebraic_index_config import AlgebraicIndexConfig
    from ..models.algebraic_index_stats import AlgebraicIndexStats
    from ..models.embeddings_index_config import EmbeddingsIndexConfig
    from ..models.embeddings_index_stats import EmbeddingsIndexStats
    from ..models.full_text_index_config import FullTextIndexConfig
    from ..models.full_text_index_stats import FullTextIndexStats
    from ..models.graph_index_config import GraphIndexConfig
    from ..models.graph_index_stats import GraphIndexStats
    from ..models.index_status_shard_status import IndexStatusShardStatus


T = TypeVar("T", bound="IndexStatus")


@_attrs_define
class IndexStatus:
    """
    Attributes:
        shard_status (IndexStatusShardStatus):
        config (AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig | GraphIndexConfig): Configuration
            for an index
        status (AlgebraicIndexStats | EmbeddingsIndexStats | FullTextIndexStats | GraphIndexStats): Statistics for an
            index
    """

    shard_status: IndexStatusShardStatus
    config: AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig | GraphIndexConfig
    status: AlgebraicIndexStats | EmbeddingsIndexStats | FullTextIndexStats | GraphIndexStats
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.embeddings_index_config import EmbeddingsIndexConfig
        from ..models.embeddings_index_stats import EmbeddingsIndexStats
        from ..models.full_text_index_config import FullTextIndexConfig
        from ..models.full_text_index_stats import FullTextIndexStats
        from ..models.graph_index_config import GraphIndexConfig
        from ..models.graph_index_stats import GraphIndexStats

        shard_status = self.shard_status.to_dict()

        config: dict[str, Any]
        if isinstance(self.config, FullTextIndexConfig):
            config = self.config.to_dict()
        elif isinstance(self.config, EmbeddingsIndexConfig):
            config = self.config.to_dict()
        elif isinstance(self.config, GraphIndexConfig):
            config = self.config.to_dict()
        else:
            config = self.config.to_dict()

        status: dict[str, Any]
        if isinstance(self.status, FullTextIndexStats):
            status = self.status.to_dict()
        elif isinstance(self.status, EmbeddingsIndexStats):
            status = self.status.to_dict()
        elif isinstance(self.status, GraphIndexStats):
            status = self.status.to_dict()
        else:
            status = self.status.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "shard_status": shard_status,
                "config": config,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.algebraic_index_config import AlgebraicIndexConfig
        from ..models.algebraic_index_stats import AlgebraicIndexStats
        from ..models.embeddings_index_config import EmbeddingsIndexConfig
        from ..models.embeddings_index_stats import EmbeddingsIndexStats
        from ..models.full_text_index_config import FullTextIndexConfig
        from ..models.full_text_index_stats import FullTextIndexStats
        from ..models.graph_index_config import GraphIndexConfig
        from ..models.graph_index_stats import GraphIndexStats
        from ..models.index_status_shard_status import IndexStatusShardStatus

        d = dict(src_dict)
        shard_status = IndexStatusShardStatus.from_dict(d.pop("shard_status"))

        def _parse_config(
            data: object,
        ) -> AlgebraicIndexConfig | EmbeddingsIndexConfig | FullTextIndexConfig | GraphIndexConfig:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_index_config_type_0 = FullTextIndexConfig.from_dict(data)

                return componentsschemas_index_config_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_index_config_type_1 = EmbeddingsIndexConfig.from_dict(data)

                return componentsschemas_index_config_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_index_config_type_2 = GraphIndexConfig.from_dict(data)

                return componentsschemas_index_config_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_index_config_type_3 = AlgebraicIndexConfig.from_dict(data)

            return componentsschemas_index_config_type_3

        config = _parse_config(d.pop("config"))

        def _parse_status(
            data: object,
        ) -> AlgebraicIndexStats | EmbeddingsIndexStats | FullTextIndexStats | GraphIndexStats:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_index_stats_type_0 = FullTextIndexStats.from_dict(data)

                return componentsschemas_index_stats_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_index_stats_type_1 = EmbeddingsIndexStats.from_dict(data)

                return componentsschemas_index_stats_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_index_stats_type_2 = GraphIndexStats.from_dict(data)

                return componentsschemas_index_stats_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_index_stats_type_3 = AlgebraicIndexStats.from_dict(data)

            return componentsschemas_index_stats_type_3

        status = _parse_status(d.pop("status"))

        index_status = cls(
            shard_status=shard_status,
            config=config,
            status=status,
        )

        index_status.additional_properties = d
        return index_status

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
