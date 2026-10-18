from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.entity_curation_result_atomicity import EntityCurationResultAtomicity

if TYPE_CHECKING:
    from ..models.entity_curation_result_results_item import EntityCurationResultResultsItem


T = TypeVar("T", bound="EntityCurationResult")


@_attrs_define
class EntityCurationResult:
    """
    Attributes:
        entity_table (str):
        key (str):
        complete (bool):
        atomicity (EntityCurationResultAtomicity):
        results (list[EntityCurationResultResultsItem]):
    """

    entity_table: str
    key: str
    complete: bool
    atomicity: EntityCurationResultAtomicity
    results: list[EntityCurationResultResultsItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        entity_table = self.entity_table

        key = self.key

        complete = self.complete

        atomicity = self.atomicity.value

        results = []
        for results_item_data in self.results:
            results_item = results_item_data.to_dict()
            results.append(results_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "entity_table": entity_table,
                "key": key,
                "complete": complete,
                "atomicity": atomicity,
                "results": results,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.entity_curation_result_results_item import EntityCurationResultResultsItem

        d = dict(src_dict)
        entity_table = d.pop("entity_table")

        key = d.pop("key")

        complete = d.pop("complete")

        atomicity = EntityCurationResultAtomicity(d.pop("atomicity"))

        results = []
        _results = d.pop("results")
        for results_item_data in _results:
            results_item = EntityCurationResultResultsItem.from_dict(results_item_data)

            results.append(results_item)

        entity_curation_result = cls(
            entity_table=entity_table,
            key=key,
            complete=complete,
            atomicity=atomicity,
            results=results,
        )

        entity_curation_result.additional_properties = d
        return entity_curation_result

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
