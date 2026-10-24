from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_recall_result_retrieval import MemoryRecallResultRetrieval
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_recall_result_entities_item import MemoryRecallResultEntitiesItem
    from ..models.memory_recall_result_episodes_item import MemoryRecallResultEpisodesItem
    from ..models.memory_recall_result_graph_type_0 import MemoryRecallResultGraphType0
    from ..models.memory_recall_result_items_item import MemoryRecallResultItemsItem


T = TypeVar("T", bound="MemoryRecallResult")


@_attrs_define
class MemoryRecallResult:
    """
    Attributes:
        items (list[MemoryRecallResultItemsItem]):
        next_cursor (None | str):
        complete (bool):
        truncated (bool):
        scanned (int):
        omitted (int):
        token_upper_bound (int):
        revision (int):
        config_generation (int):
        retrieval (MemoryRecallResultRetrieval):
        entities (list[MemoryRecallResultEntitiesItem] | Unset):
        episodes (list[MemoryRecallResultEpisodesItem] | Unset):
        graph (MemoryRecallResultGraphType0 | None | Unset):
    """

    items: list[MemoryRecallResultItemsItem]
    next_cursor: None | str
    complete: bool
    truncated: bool
    scanned: int
    omitted: int
    token_upper_bound: int
    revision: int
    config_generation: int
    retrieval: MemoryRecallResultRetrieval
    entities: list[MemoryRecallResultEntitiesItem] | Unset = UNSET
    episodes: list[MemoryRecallResultEpisodesItem] | Unset = UNSET
    graph: MemoryRecallResultGraphType0 | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_recall_result_graph_type_0 import MemoryRecallResultGraphType0

        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        next_cursor: None | str
        next_cursor = self.next_cursor

        complete = self.complete

        truncated = self.truncated

        scanned = self.scanned

        omitted = self.omitted

        token_upper_bound = self.token_upper_bound

        revision = self.revision

        config_generation = self.config_generation

        retrieval = self.retrieval.value

        entities: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.entities, Unset):
            entities = []
            for entities_item_data in self.entities:
                entities_item = entities_item_data.to_dict()
                entities.append(entities_item)

        episodes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.episodes, Unset):
            episodes = []
            for episodes_item_data in self.episodes:
                episodes_item = episodes_item_data.to_dict()
                episodes.append(episodes_item)

        graph: dict[str, Any] | None | Unset
        if isinstance(self.graph, Unset):
            graph = UNSET
        elif isinstance(self.graph, MemoryRecallResultGraphType0):
            graph = self.graph.to_dict()
        else:
            graph = self.graph

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "items": items,
                "next_cursor": next_cursor,
                "complete": complete,
                "truncated": truncated,
                "scanned": scanned,
                "omitted": omitted,
                "token_upper_bound": token_upper_bound,
                "revision": revision,
                "config_generation": config_generation,
                "retrieval": retrieval,
            }
        )
        if entities is not UNSET:
            field_dict["entities"] = entities
        if episodes is not UNSET:
            field_dict["episodes"] = episodes
        if graph is not UNSET:
            field_dict["graph"] = graph

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_recall_result_entities_item import MemoryRecallResultEntitiesItem
        from ..models.memory_recall_result_episodes_item import MemoryRecallResultEpisodesItem
        from ..models.memory_recall_result_graph_type_0 import MemoryRecallResultGraphType0
        from ..models.memory_recall_result_items_item import MemoryRecallResultItemsItem

        d = dict(src_dict)
        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = MemoryRecallResultItemsItem.from_dict(items_item_data)

            items.append(items_item)

        def _parse_next_cursor(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor"))

        complete = d.pop("complete")

        truncated = d.pop("truncated")

        scanned = d.pop("scanned")

        omitted = d.pop("omitted")

        token_upper_bound = d.pop("token_upper_bound")

        revision = d.pop("revision")

        config_generation = d.pop("config_generation")

        retrieval = MemoryRecallResultRetrieval(d.pop("retrieval"))

        _entities = d.pop("entities", UNSET)
        entities: list[MemoryRecallResultEntitiesItem] | Unset = UNSET
        if _entities is not UNSET:
            entities = []
            for entities_item_data in _entities:
                entities_item = MemoryRecallResultEntitiesItem.from_dict(entities_item_data)

                entities.append(entities_item)

        _episodes = d.pop("episodes", UNSET)
        episodes: list[MemoryRecallResultEpisodesItem] | Unset = UNSET
        if _episodes is not UNSET:
            episodes = []
            for episodes_item_data in _episodes:
                episodes_item = MemoryRecallResultEpisodesItem.from_dict(episodes_item_data)

                episodes.append(episodes_item)

        def _parse_graph(data: object) -> MemoryRecallResultGraphType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                graph_type_0 = MemoryRecallResultGraphType0.from_dict(data)

                return graph_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryRecallResultGraphType0 | None | Unset, data)

        graph = _parse_graph(d.pop("graph", UNSET))

        memory_recall_result = cls(
            items=items,
            next_cursor=next_cursor,
            complete=complete,
            truncated=truncated,
            scanned=scanned,
            omitted=omitted,
            token_upper_bound=token_upper_bound,
            revision=revision,
            config_generation=config_generation,
            retrieval=retrieval,
            entities=entities,
            episodes=episodes,
            graph=graph,
        )

        return memory_recall_result
