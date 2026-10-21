from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_resolver_config import GraphResolverConfig
    from ..models.resolver_curation_state import ResolverCurationState
    from ..models.resolver_redirect import ResolverRedirect
    from ..models.resolver_replay_progress import ResolverReplayProgress


T = TypeVar("T", bound="ResolverSummary")


@_attrs_define
class ResolverSummary:
    """
    Attributes:
        name (str):
        source_table (str):
        entity_table (str):
        declaring_indexes (list[str]):
        retired (bool):
        revision (int):
        redirects (list[ResolverRedirect]):
        curation (ResolverCurationState):
        key_template (str | Unset):
        config (GraphResolverConfig | None | Unset):
        namespace_generation (int | Unset):
        progress (ResolverReplayProgress | Unset):
    """

    name: str
    source_table: str
    entity_table: str
    declaring_indexes: list[str]
    retired: bool
    revision: int
    redirects: list[ResolverRedirect]
    curation: ResolverCurationState
    key_template: str | Unset = UNSET
    config: GraphResolverConfig | None | Unset = UNSET
    namespace_generation: int | Unset = UNSET
    progress: ResolverReplayProgress | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_resolver_config import GraphResolverConfig

        name = self.name

        source_table = self.source_table

        entity_table = self.entity_table

        declaring_indexes = self.declaring_indexes

        retired = self.retired

        revision = self.revision

        redirects = []
        for redirects_item_data in self.redirects:
            redirects_item = redirects_item_data.to_dict()
            redirects.append(redirects_item)

        curation = self.curation.to_dict()

        key_template = self.key_template

        config: dict[str, Any] | None | Unset
        if isinstance(self.config, Unset):
            config = UNSET
        elif isinstance(self.config, GraphResolverConfig):
            config = self.config.to_dict()
        else:
            config = self.config

        namespace_generation = self.namespace_generation

        progress: dict[str, Any] | Unset = UNSET
        if not isinstance(self.progress, Unset):
            progress = self.progress.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "source_table": source_table,
                "entity_table": entity_table,
                "declaring_indexes": declaring_indexes,
                "retired": retired,
                "revision": revision,
                "redirects": redirects,
                "curation": curation,
            }
        )
        if key_template is not UNSET:
            field_dict["key_template"] = key_template
        if config is not UNSET:
            field_dict["config"] = config
        if namespace_generation is not UNSET:
            field_dict["namespace_generation"] = namespace_generation
        if progress is not UNSET:
            field_dict["progress"] = progress

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_resolver_config import GraphResolverConfig
        from ..models.resolver_curation_state import ResolverCurationState
        from ..models.resolver_redirect import ResolverRedirect
        from ..models.resolver_replay_progress import ResolverReplayProgress

        d = dict(src_dict)
        name = d.pop("name")

        source_table = d.pop("source_table")

        entity_table = d.pop("entity_table")

        declaring_indexes = cast(list[str], d.pop("declaring_indexes"))

        retired = d.pop("retired")

        revision = d.pop("revision")

        redirects = []
        _redirects = d.pop("redirects")
        for redirects_item_data in _redirects:
            redirects_item = ResolverRedirect.from_dict(redirects_item_data)

            redirects.append(redirects_item)

        curation = ResolverCurationState.from_dict(d.pop("curation"))

        key_template = d.pop("key_template", UNSET)

        def _parse_config(data: object) -> GraphResolverConfig | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                config_type_1 = GraphResolverConfig.from_dict(data)

                return config_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(GraphResolverConfig | None | Unset, data)

        config = _parse_config(d.pop("config", UNSET))

        namespace_generation = d.pop("namespace_generation", UNSET)

        _progress = d.pop("progress", UNSET)
        progress: ResolverReplayProgress | Unset
        if isinstance(_progress, Unset):
            progress = UNSET
        else:
            progress = ResolverReplayProgress.from_dict(_progress)

        resolver_summary = cls(
            name=name,
            source_table=source_table,
            entity_table=entity_table,
            declaring_indexes=declaring_indexes,
            retired=retired,
            revision=revision,
            redirects=redirects,
            curation=curation,
            key_template=key_template,
            config=config,
            namespace_generation=namespace_generation,
            progress=progress,
        )

        resolver_summary.additional_properties = d
        return resolver_summary

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
