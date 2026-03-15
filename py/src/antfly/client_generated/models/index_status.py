from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
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
        config (Union['EmbeddingsIndexConfig', 'FullTextIndexConfig', 'GraphIndexConfig']): Configuration for an index
        status (Union['EmbeddingsIndexStats', 'FullTextIndexStats', 'GraphIndexStats']): Statistics for an index
    """

    shard_status: "IndexStatusShardStatus"
    config: Union["EmbeddingsIndexConfig", "FullTextIndexConfig", "GraphIndexConfig"]
    status: Union["EmbeddingsIndexStats", "FullTextIndexStats", "GraphIndexStats"]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.embeddings_index_config import EmbeddingsIndexConfig
        from ..models.embeddings_index_stats import EmbeddingsIndexStats
        from ..models.full_text_index_config import FullTextIndexConfig
        from ..models.full_text_index_stats import FullTextIndexStats

        shard_status = self.shard_status.to_dict()

        config: dict[str, Any]
        if isinstance(self.config, FullTextIndexConfig):
            config = self.config.to_dict()
        elif isinstance(self.config, EmbeddingsIndexConfig):
            config = self.config.to_dict()
        else:
            config = self.config.to_dict()

        status: dict[str, Any]
        if isinstance(self.status, FullTextIndexStats):
            status = self.status.to_dict()
        elif isinstance(self.status, EmbeddingsIndexStats):
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
        from ..models.embeddings_index_config import EmbeddingsIndexConfig
        from ..models.embeddings_index_stats import EmbeddingsIndexStats
        from ..models.full_text_index_config import FullTextIndexConfig
        from ..models.full_text_index_stats import FullTextIndexStats
        from ..models.graph_index_config import GraphIndexConfig
        from ..models.graph_index_stats import GraphIndexStats
        from ..models.index_status_shard_status import IndexStatusShardStatus

        d = dict(src_dict)
        shard_status = IndexStatusShardStatus.from_dict(d.pop("shard_status"))

        def _parse_config(data: object) -> Union["EmbeddingsIndexConfig", "FullTextIndexConfig", "GraphIndexConfig"]:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_index_config_type_0 = FullTextIndexConfig.from_dict(data)

                return componentsschemas_index_config_type_0
            except:  # noqa: E722
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_index_config_type_1 = EmbeddingsIndexConfig.from_dict(data)

                return componentsschemas_index_config_type_1
            except:  # noqa: E722
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_index_config_type_2 = GraphIndexConfig.from_dict(data)

            return componentsschemas_index_config_type_2

        config = _parse_config(d.pop("config"))

        def _parse_status(data: object) -> Union["EmbeddingsIndexStats", "FullTextIndexStats", "GraphIndexStats"]:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_index_stats_type_0 = FullTextIndexStats.from_dict(data)

                return componentsschemas_index_stats_type_0
            except:  # noqa: E722
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_index_stats_type_1 = EmbeddingsIndexStats.from_dict(data)

                return componentsschemas_index_stats_type_1
            except:  # noqa: E722
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_index_stats_type_2 = GraphIndexStats.from_dict(data)

            return componentsschemas_index_stats_type_2

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
