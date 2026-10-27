from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.entity_consumer_progress_page_results_item import EntityConsumerProgressPageResultsItem


T = TypeVar("T", bound="EntityConsumerProgressPage")


@_attrs_define
class EntityConsumerProgressPage:
    """
    Attributes:
        entity_table (str):
        key (str):
        catalog_revision (int):
        complete (bool): True when enumeration has no further pages; it does not certify replay completion.
        results (list[EntityConsumerProgressPageResultsItem]):
        next_cursor (None | str | Unset):
    """

    entity_table: str
    key: str
    catalog_revision: int
    complete: bool
    results: list[EntityConsumerProgressPageResultsItem]
    next_cursor: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        entity_table = self.entity_table

        key = self.key

        catalog_revision = self.catalog_revision

        complete = self.complete

        results = []
        for results_item_data in self.results:
            results_item = results_item_data.to_dict()
            results.append(results_item)

        next_cursor: None | str | Unset
        if isinstance(self.next_cursor, Unset):
            next_cursor = UNSET
        else:
            next_cursor = self.next_cursor

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "entity_table": entity_table,
                "key": key,
                "catalog_revision": catalog_revision,
                "complete": complete,
                "results": results,
            }
        )
        if next_cursor is not UNSET:
            field_dict["next_cursor"] = next_cursor

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.entity_consumer_progress_page_results_item import EntityConsumerProgressPageResultsItem

        d = dict(src_dict)
        entity_table = d.pop("entity_table")

        key = d.pop("key")

        catalog_revision = d.pop("catalog_revision")

        complete = d.pop("complete")

        results = []
        _results = d.pop("results")
        for results_item_data in _results:
            results_item = EntityConsumerProgressPageResultsItem.from_dict(results_item_data)

            results.append(results_item)

        def _parse_next_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor", UNSET))

        entity_consumer_progress_page = cls(
            entity_table=entity_table,
            key=key,
            catalog_revision=catalog_revision,
            complete=complete,
            results=results,
            next_cursor=next_cursor,
        )

        entity_consumer_progress_page.additional_properties = d
        return entity_consumer_progress_page

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
