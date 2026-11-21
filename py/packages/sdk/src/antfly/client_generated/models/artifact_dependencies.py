from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.artifact_dependencies_freshness_type_0 import ArtifactDependenciesFreshnessType0
    from ..models.artifact_dependencies_versions_item import ArtifactDependenciesVersionsItem


T = TypeVar("T", bound="ArtifactDependencies")


@_attrs_define
class ArtifactDependencies:
    """Dependency state of one document artifact whose enrichment declares
    `context_sources`. `freshness` is null when the artifact was never
    generated with context dependencies. Freshness (`current`, `stale`,
    `failed`, `pending`) is reported separately from rebuild execution
    state. `versions` lists retained lineage manifests, newest first.

        Attributes:
            freshness (ArtifactDependenciesFreshnessType0 | None): State, generation, published generation, version,
                validated-through frontier, input fingerprint, rebuild state, and bounded change reasons.
            versions (list[ArtifactDependenciesVersionsItem]):
            subscriptions (int | Unset): Live subscription tuples registered for this artifact.
            current (bool | Unset): Present when `freshness=require_current` was requested: true when a
                read barrier observed no undelivered committed change to any of the
                artifact's source tables and its dependency state is current.
    """

    freshness: ArtifactDependenciesFreshnessType0 | None
    versions: list[ArtifactDependenciesVersionsItem]
    subscriptions: int | Unset = UNSET
    current: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.artifact_dependencies_freshness_type_0 import ArtifactDependenciesFreshnessType0

        freshness: dict[str, Any] | None
        if isinstance(self.freshness, ArtifactDependenciesFreshnessType0):
            freshness = self.freshness.to_dict()
        else:
            freshness = self.freshness

        versions = []
        for versions_item_data in self.versions:
            versions_item = versions_item_data.to_dict()
            versions.append(versions_item)

        subscriptions = self.subscriptions

        current = self.current

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "freshness": freshness,
                "versions": versions,
            }
        )
        if subscriptions is not UNSET:
            field_dict["subscriptions"] = subscriptions
        if current is not UNSET:
            field_dict["current"] = current

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.artifact_dependencies_freshness_type_0 import ArtifactDependenciesFreshnessType0
        from ..models.artifact_dependencies_versions_item import ArtifactDependenciesVersionsItem

        d = dict(src_dict)

        def _parse_freshness(data: object) -> ArtifactDependenciesFreshnessType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                freshness_type_0 = ArtifactDependenciesFreshnessType0.from_dict(data)

                return freshness_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ArtifactDependenciesFreshnessType0 | None, data)

        freshness = _parse_freshness(d.pop("freshness"))

        versions = []
        _versions = d.pop("versions")
        for versions_item_data in _versions:
            versions_item = ArtifactDependenciesVersionsItem.from_dict(versions_item_data)

            versions.append(versions_item)

        subscriptions = d.pop("subscriptions", UNSET)

        current = d.pop("current", UNSET)

        artifact_dependencies = cls(
            freshness=freshness,
            versions=versions,
            subscriptions=subscriptions,
            current=current,
        )

        artifact_dependencies.additional_properties = d
        return artifact_dependencies

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
