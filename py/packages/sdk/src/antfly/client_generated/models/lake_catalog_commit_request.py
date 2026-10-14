from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.lake_catalog_commit_request_requirements_item import LakeCatalogCommitRequestRequirementsItem
    from ..models.lake_catalog_commit_request_updates_item import LakeCatalogCommitRequestUpdatesItem


T = TypeVar("T", bound="LakeCatalogCommitRequest")


@_attrs_define
class LakeCatalogCommitRequest:
    """
    Attributes:
        commit_id (str):
        expected_metadata_location (str):
        requirements (list[LakeCatalogCommitRequestRequirementsItem]): Standard Iceberg REST table requirements,
            validated against the authoritative state.
        updates (list[LakeCatalogCommitRequestUpdatesItem]): Standard Iceberg REST metadata updates. Upload
            data/delete/manifest files before committing. A lake commit does not establish Antfly index visibility.
    """

    commit_id: str
    expected_metadata_location: str
    requirements: list[LakeCatalogCommitRequestRequirementsItem]
    updates: list[LakeCatalogCommitRequestUpdatesItem]

    def to_dict(self) -> dict[str, Any]:
        commit_id = self.commit_id

        expected_metadata_location = self.expected_metadata_location

        requirements = []
        for requirements_item_data in self.requirements:
            requirements_item = requirements_item_data.to_dict()
            requirements.append(requirements_item)

        updates = []
        for updates_item_data in self.updates:
            updates_item = updates_item_data.to_dict()
            updates.append(updates_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "commit_id": commit_id,
                "expected_metadata_location": expected_metadata_location,
                "requirements": requirements,
                "updates": updates,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.lake_catalog_commit_request_requirements_item import LakeCatalogCommitRequestRequirementsItem
        from ..models.lake_catalog_commit_request_updates_item import LakeCatalogCommitRequestUpdatesItem

        d = dict(src_dict)
        commit_id = d.pop("commit_id")

        expected_metadata_location = d.pop("expected_metadata_location")

        requirements = []
        _requirements = d.pop("requirements")
        for requirements_item_data in _requirements:
            requirements_item = LakeCatalogCommitRequestRequirementsItem.from_dict(requirements_item_data)

            requirements.append(requirements_item)

        updates = []
        _updates = d.pop("updates")
        for updates_item_data in _updates:
            updates_item = LakeCatalogCommitRequestUpdatesItem.from_dict(updates_item_data)

            updates.append(updates_item)

        lake_catalog_commit_request = cls(
            commit_id=commit_id,
            expected_metadata_location=expected_metadata_location,
            requirements=requirements,
            updates=updates,
        )

        return lake_catalog_commit_request
