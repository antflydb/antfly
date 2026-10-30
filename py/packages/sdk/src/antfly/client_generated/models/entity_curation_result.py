from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.entity_curation_result_atomicity import EntityCurationResultAtomicity
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.entity_curation_result_results_item import EntityCurationResultResultsItem
    from ..models.entity_namespace_outcome import EntityNamespaceOutcome


T = TypeVar("T", bound="EntityCurationResult")


@_attrs_define
class EntityCurationResult:
    """
    Attributes:
        entity_table (str):
        key (str):
        complete (bool):
        atomicity (EntityCurationResultAtomicity): Entity corrections commit atomically across matching namespaces on
            the destination table; replay is asynchronous. Other atomicity values identify the scope guaranteed by the
            responding adapter.
        results (list[EntityCurationResultResultsItem]):
        applied (bool | Unset): True only when replay completion was observed for all consumers, or no consumers exist.
            Admission without wait is conservative.
        progress_url (str | Unset): Native HTTP mutations return this progress resource relative to the request URI.
            Follow its bounded pages for individual consumer proofs.
        namespaces (list[EntityNamespaceOutcome] | Unset): Mutation outcomes, stored once per namespace. Read absorbed
            keys and dispositions here; consumer progress rows reference these by key_template. The results array is
            reserved for inspection and embedded consumer summaries.
    """

    entity_table: str
    key: str
    complete: bool
    atomicity: EntityCurationResultAtomicity
    results: list[EntityCurationResultResultsItem]
    applied: bool | Unset = UNSET
    progress_url: str | Unset = UNSET
    namespaces: list[EntityNamespaceOutcome] | Unset = UNSET
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

        applied = self.applied

        progress_url = self.progress_url

        namespaces: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.namespaces, Unset):
            namespaces = []
            for namespaces_item_data in self.namespaces:
                namespaces_item = namespaces_item_data.to_dict()
                namespaces.append(namespaces_item)

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
        if applied is not UNSET:
            field_dict["applied"] = applied
        if progress_url is not UNSET:
            field_dict["progress_url"] = progress_url
        if namespaces is not UNSET:
            field_dict["namespaces"] = namespaces

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.entity_curation_result_results_item import EntityCurationResultResultsItem
        from ..models.entity_namespace_outcome import EntityNamespaceOutcome

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

        applied = d.pop("applied", UNSET)

        progress_url = d.pop("progress_url", UNSET)

        _namespaces = d.pop("namespaces", UNSET)
        namespaces: list[EntityNamespaceOutcome] | Unset = UNSET
        if _namespaces is not UNSET:
            namespaces = []
            for namespaces_item_data in _namespaces:
                namespaces_item = EntityNamespaceOutcome.from_dict(namespaces_item_data)

                namespaces.append(namespaces_item)

        entity_curation_result = cls(
            entity_table=entity_table,
            key=key,
            complete=complete,
            atomicity=atomicity,
            results=results,
            applied=applied,
            progress_url=progress_url,
            namespaces=namespaces,
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
